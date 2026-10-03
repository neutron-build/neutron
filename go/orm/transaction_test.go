package orm

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Lifecycle driver fixtures test branches that cannot be induced reliably by
// a live server (for example losing the COMMIT response after transmission).
// They do not establish database atomicity; separate PostgreSQL tests do that.
type lifecycleDriver struct {
	commitErr, rollbackErr    error
	commits, rollbacks, execs int
	statements                []string
}

func (d *lifecycleDriver) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &fixtureRows{}, nil
}
func (d *lifecycleDriver) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	d.execs++
	d.statements = append(d.statements, sql)
	return pgconn.NewCommandTag("SELECT 1"), nil
}
func (d *lifecycleDriver) Commit(context.Context) error   { d.commits++; return d.commitErr }
func (d *lifecycleDriver) Rollback(context.Context) error { d.rollbacks++; return d.rollbackErr }

type retrySafeFailure struct{}

func (retrySafeFailure) Error() string     { return "failed before any bytes sent" }
func (retrySafeFailure) SafeToRetry() bool { return true }

func fixtureOwner(ctx context.Context, driver transactionDriver) (*transactionOwner, *Scope, *int, *int) {
	releases, discards := new(int), new(int)
	o := &transactionOwner{ctx: ctx, driver: driver, cleanupTimeout: time.Second, release: func() { *releases++ }, discard: func(context.Context, <-chan struct{}) error { *discards++; return nil }}
	s := &Scope{owner: o}
	o.current = s
	o.scopes = []*Scope{s}
	return o, s, releases, discards
}

func TestCommitOutcomeClassificationNoReplay(t *testing.T) {
	cases := []struct {
		name    string
		cause   error
		outcome CommitOutcome
	}{
		{"transport", errors.New("lost COMMIT response"), CommitUnknown},
		{"inflight_cancel", context.Canceled, CommitUnknown},
		{"08007", &pgconn.PgError{Code: "08007", Message: "transaction_resolution_unknown"}, CommitUnknown},
		{"08006", &pgconn.PgError{Code: "08006"}, CommitUnknown},
		{"40003", &pgconn.PgError{Code: "40003", Message: "statement_completion_unknown"}, CommitUnknown},
		{"other_state", &pgconn.PgError{Code: "57014"}, CommitUnknown},
		{"serialization", &pgconn.PgError{Code: "40001"}, CommitRejected},
		{"deferred_unique", &pgconn.PgError{Code: "23505"}, CommitRejected},
		{"rollback_tag", pgx.ErrTxCommitRollback, CommitRejected},
		{"not_sent", retrySafeFailure{}, CommitNotAttempted},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := &lifecycleDriver{commitErr: c.cause}
			o, s, releases, discards := fixtureOwner(context.Background(), d)
			err := o.finishRoot(s, nil)
			var txErr *TransactionError
			if !errors.As(err, &txErr) || txErr.Outcome != c.outcome || !errors.Is(err, c.cause) {
				t.Fatal(err)
			}
			if d.commits != 1 || d.rollbacks != 0 || *releases != 0 || *discards != 1 {
				t.Fatal("retry/rollback/recycle after failed COMMIT", d, *releases, *discards)
			}
			if errors.Is(err, ErrCommitAmbiguous) != (c.outcome == CommitUnknown) {
				t.Fatal("ambiguity marker", err)
			}
			if _, err := s.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrScopeClosed) {
				t.Fatal("terminal handle accepted", err)
			}
		})
	}
}

func TestPrecommitCancellationAndCleanupFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	d := &lifecycleDriver{}
	o, s, releases, discards := fixtureOwner(ctx, d)
	cancel()
	err := o.finishRoot(s, nil)
	var txErr *TransactionError
	if !errors.As(err, &txErr) || txErr.Outcome != CommitNotAttempted || !errors.Is(err, context.Canceled) || d.commits != 0 || d.rollbacks != 1 || *releases != 1 || *discards != 0 {
		t.Fatal(err, d)
	}
	callbackErr := errors.New("callback failed")
	d = &lifecycleDriver{rollbackErr: errors.New("transport during rollback")}
	o, s, releases, discards = fixtureOwner(context.Background(), d)
	err = o.finishRoot(s, callbackErr)
	if !errors.Is(err, callbackErr) || !errors.Is(err, ErrTransactionBroken) || d.commits != 0 || d.rollbacks != 1 || *releases != 0 || *discards != 1 {
		t.Fatal("failed cleanup recycled", err, d)
	}
}

type releaseFailureDriver struct{ lifecycleDriver }

func (d *releaseFailureDriver) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.HasPrefix(sql, "RELEASE SAVEPOINT") {
		return pgconn.CommandTag{}, errors.New("savepoint release transport failure")
	}
	return d.lifecycleDriver.Exec(ctx, sql, args...)
}

func TestFailedSavepointCleanupDiscardsEvenAfterRootRollback(t *testing.T) {
	d := &releaseFailureDriver{}
	o, parent, releases, discards := fixtureOwner(context.Background(), d)
	err := parent.Savepoint(context.Background(), func(*Scope) error { return nil })
	if !errors.Is(err, ErrTransactionBroken) {
		t.Fatal("failed release not marked broken", err)
	}
	if _, err := parent.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrTransactionBroken) {
		t.Fatal("broken parent admitted work", err)
	}
	// Even a swallowed child failure cannot become success or recycle a
	// connection whose savepoint cleanup was not acknowledged.
	err = o.finishRoot(parent, nil)
	if !errors.Is(err, ErrTransactionBroken) || d.commits != 0 || d.rollbacks != 1 || *releases != 0 || *discards != 1 {
		t.Fatal(err, d, *releases, *discards)
	}
}

func TestTransactionOptions(t *testing.T) {
	valid := TransactionOptions{Isolation: pgx.Serializable, Access: pgx.ReadOnly, Deferrable: pgx.Deferrable}
	if _, _, err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	for _, o := range []TransactionOptions{{Isolation: "injected"}, {Access: "invalid"}, {Deferrable: "invalid"}, {Deferrable: pgx.Deferrable}, {Isolation: pgx.Serializable, Access: pgx.ReadWrite, Deferrable: pgx.Deferrable}, {CleanupTimeout: -time.Second}} {
		if _, _, err := o.validate(); err == nil {
			t.Fatal("invalid transaction options accepted", o)
		}
	}
}

func TestScopeStatementAdmission(t *testing.T) {
	for _, sql := range []string{"SELECT $1", `SELECT '; COMMIT'`, "/* outer /* nested */ */ SELECT 1; -- end", `SELECT $$ ; COMMIT $$`, `SELECT E'escaped\\\'quote';`, `SELECT "odd;column" FROM "schema"."table"`, "WITH x AS (SELECT 1) SELECT * FROM x"} {
		if err := validateScopeSQL(sql); err != nil {
			t.Fatal("safe single data statement refused", sql, err)
		}
	}
	for _, sql := range []string{"COMMIT", "ROLLBACK", "SAVEPOINT x", "SET TRANSACTION READ WRITE", "CALL procedure()", "DO $$ BEGIN END $$", "SELECT 1; COMMIT", "SELECT 1;;", `SELECT '\'; COMMIT`, "/* unterminated", "SELECT $tag$unterminated"} {
		if err := validateScopeSQL(sql); err == nil {
			t.Fatal("control/ambiguous statement accepted", sql)
		}
	}
	if err := validateScopeArguments([]any{pgx.NamedArgs{"id": 1}}); err == nil {
		t.Fatal("query rewriter can bypass statement admission")
	}
}

type fixtureRows struct{ closed bool }

func (r *fixtureRows) Close()                                     { r.closed = true }
func (*fixtureRows) Err() error                                   { return nil }
func (*fixtureRows) CommandTag() pgconn.CommandTag                { return pgconn.NewCommandTag("SELECT 0") }
func (*fixtureRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (*fixtureRows) Next() bool                                   { return false }
func (*fixtureRows) Scan(...any) error                            { return nil }
func (*fixtureRows) Values() ([]any, error)                       { return nil, nil }
func (*fixtureRows) RawValues() [][]byte                          { return nil }
func (*fixtureRows) Conn() *pgx.Conn                              { return nil }

type blockingQueryDriver struct {
	lifecycleDriver
	started chan struct{}
	unblock chan struct{}
}

func (d *blockingQueryDriver) Query(ctx context.Context, _ string, _ ...any) (pgx.Rows, error) {
	close(d.started)
	select {
	case <-d.unblock:
		return &fixtureRows{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestScopeLeaseConcurrentTerminalAndChildOwnership(t *testing.T) {
	d := &blockingQueryDriver{started: make(chan struct{}), unblock: make(chan struct{})}
	_, s, _, _ := fixtureOwner(context.Background(), d)
	result := make(chan pgx.Rows, 1)
	fail := make(chan error, 1)
	go func() {
		rows, err := s.Query(context.Background(), "SELECT 1")
		if err != nil {
			fail <- err
			return
		}
		result <- rows
	}()
	select {
	case <-d.started:
	case <-time.After(time.Second):
		t.Fatal("query did not start")
	}
	if _, err := s.Exec(context.Background(), "SELECT 2"); !errors.Is(err, ErrConcurrentUse) || d.execs != 0 {
		t.Fatal("concurrent native command admitted", err)
	}
	close(d.unblock)
	var rows pgx.Rows
	select {
	case rows = <-result:
	case err := <-fail:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("query did not return")
	}
	if _, err := s.Exec(context.Background(), "SELECT 2"); !errors.Is(err, ErrConcurrentUse) {
		t.Fatal("rows lifetime released lease early", err)
	}
	if rows.Conn() != nil {
		t.Fatal("native connection escape exposed")
	}
	rows.Close()
	base := &lifecycleDriver{}
	o, parent, _, _ := fixtureOwner(context.Background(), base)
	sentinel := errors.New("child rollback")
	var child *Scope
	err := parent.Savepoint(context.Background(), func(scope *Scope) error {
		child = scope
		if _, err := parent.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrParentSuspended) {
			return errors.New("parent was not suspended")
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) || o.current != parent || !strings.Contains(strings.Join(base.statements, ";"), "ROLLBACK TO SAVEPOINT") {
		t.Fatal(err, base.statements)
	}
	if _, err := child.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrScopeClosed) {
		t.Fatal("child handle survived callback", err)
	}
	if _, err := parent.Exec(context.Background(), "SELECT 1"); err != nil {
		t.Fatal("parent did not resume", err)
	}
	if err := o.finishRoot(parent, sentinel); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}
