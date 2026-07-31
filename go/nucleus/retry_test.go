package nucleus

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func pgErr(code string) error {
	return &pgconn.PgError{Code: code, Message: "synthetic"}
}

// The whole point of these helpers is that they classify by SQLSTATE and not by
// message text. A driver surfaces the code; the message is free-form and
// changes. Nucleus itself shipped this bug twice — a 2PL kill reported as
// XX000, then its follow-up error reported as XX000 — so the client half of the
// contract gets a test.
func TestIsSerializationFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"40001 serialization failure", pgErr(SQLStateSerializationFailure), true},
		{"25P02 in failed transaction", pgErr(SQLStateInFailedTransaction), true},
		{"55P03 lock timeout is NOT retryable", pgErr(SQLStateLockNotAvailable), false},
		{"23505 unique violation", pgErr("23505"), false},
		{"XX000 internal", pgErr("XX000"), false},
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
		{"wrapped 40001", fmt.Errorf("outer: %w", pgErr(SQLStateSerializationFailure)), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsSerializationFailure(tc.err); got != tc.want {
				t.Fatalf("IsSerializationFailure(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// A lock timeout means the holder is still there. Retrying spins against a lock
// that is not moving, which turns one stuck transaction into a busy loop — so
// it must be distinguishable from a conflict, not lumped in with it.
func TestIsLockNotAvailable(t *testing.T) {
	if !IsLockNotAvailable(pgErr(SQLStateLockNotAvailable)) {
		t.Fatal("55P03 should be recognised as a lock timeout")
	}
	if IsLockNotAvailable(pgErr(SQLStateSerializationFailure)) {
		t.Fatal("40001 must not be classified as a lock timeout")
	}
	if IsSerializationFailure(pgErr(SQLStateLockNotAvailable)) {
		t.Fatal("a lock timeout must never be retried as a serialization failure")
	}
	if IsLockNotAvailable(errors.New("boom")) {
		t.Fatal("a non-pg error is not a lock timeout")
	}
}

// Wrapping matters because every layer between the driver and the application
// adds context with %w. Classification that only works on the bare driver error
// works nowhere real.
func TestClassificationSeesThroughWrapping(t *testing.T) {
	deep := fmt.Errorf("repo: %w", fmt.Errorf("service: %w", pgErr(SQLStateSerializationFailure)))
	if !IsSerializationFailure(deep) {
		t.Fatal("classification must unwrap")
	}
}

func TestDefaultRetryOptionsAreSane(t *testing.T) {
	o := DefaultRetryOptions()
	if o.MaxAttempts < 2 {
		t.Fatalf("MaxAttempts = %d; a retry helper that retries zero times is not one", o.MaxAttempts)
	}
	if o.BaseDelay <= 0 || o.MaxDelay < o.BaseDelay {
		t.Fatalf("nonsensical backoff: base=%v max=%v", o.BaseDelay, o.MaxDelay)
	}
}
