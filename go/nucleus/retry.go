package nucleus

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// SQLSTATE codes a Nucleus transaction can fail with that the caller is
// expected to act on.
const (
	// SQLStateSerializationFailure (40001) — the transaction lost a conflict
	// and MUST be retried from the beginning. Nucleus raises it from two
	// mechanisms: strict 2PL wait-die on the disk engine (the younger
	// transaction is killed to break a potential deadlock) and SSI on the MVCC
	// engine (a dangerous structure was detected at commit).
	SQLStateSerializationFailure = "40001"

	// SQLStateLockNotAvailable (55P03) — `lock_timeout` elapsed while waiting
	// for a table lock. Deliberately NOT retryable: the holder is still there,
	// so retrying spins against a lock that is not moving. Raise lock_timeout
	// or find the transaction holding it.
	SQLStateLockNotAvailable = "55P03"

	// SQLStateInFailedTransaction (25P02) — a statement was issued after the
	// transaction had already been aborted. The transaction is dead; only
	// ROLLBACK is accepted. Retryable only in the sense that the whole
	// transaction must be re-run, which is what WithTx does.
	SQLStateInFailedTransaction = "25P02"
)

// sqlState extracts the SQLSTATE from a driver error, or "" if there is none.
func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// IsSerializationFailure reports whether err is a conflict the caller should
// retry (SQLSTATE 40001).
//
// This is the check every application running SERIALIZABLE needs and that no
// PostgreSQL driver performs for you: drivers surface the code, the
// application decides. A serializable transaction that is never retried is a
// transaction that randomly fails under concurrency.
func IsSerializationFailure(err error) bool {
	code := sqlState(err)
	return code == SQLStateSerializationFailure || code == SQLStateInFailedTransaction
}

// IsLockNotAvailable reports whether err is a lock_timeout expiry (55P03).
//
// Kept distinct from IsSerializationFailure on purpose. The two call for
// opposite responses: a serialization failure means "someone else won, try
// again"; a lock timeout means "the lock is still held, retrying will not help
// — raise lock_timeout or go find the holder".
func IsLockNotAvailable(err error) bool {
	return sqlState(err) == SQLStateLockNotAvailable
}

// RetryOptions controls WithTx's retry behaviour.
type RetryOptions struct {
	// MaxAttempts including the first. Values below 1 are treated as 1.
	MaxAttempts int
	// BaseDelay before the second attempt; doubled each subsequent attempt.
	BaseDelay time.Duration
	// MaxDelay caps the backoff.
	MaxDelay time.Duration
	// IsolationLevel, e.g. "SERIALIZABLE". Empty leaves the server default.
	IsolationLevel string
}

// DefaultRetryOptions are used when WithTx is given a nil *RetryOptions.
//
// Backoff is randomised (full jitter). Without it, two transactions that
// conflict retry in lockstep and collide again on the same schedule — under
// wait-die the younger one loses every round, so a fixed backoff can starve it
// indefinitely.
func DefaultRetryOptions() *RetryOptions {
	return &RetryOptions{
		MaxAttempts: 5,
		BaseDelay:   2 * time.Millisecond,
		MaxDelay:    250 * time.Millisecond,
	}
}

// WithTx runs fn inside a transaction, retrying it on serialization failure.
//
// This is the managed-transaction entry point the SDK was missing. Begin /
// Commit / Rollback remain available for callers that want to drive the
// boundary themselves, but they put the retry obligation on every call site,
// and the audit that prompted this found no caller anywhere in the tree that
// met it.
//
// fn MUST be idempotent with respect to anything outside the database: it can
// run more than once. Everything it does through the passed *Tx is rolled back
// between attempts; anything it does elsewhere (sending mail, charging a card,
// mutating a package-level variable) is not.
//
// On success the transaction is committed. On a serialization failure it is
// rolled back and retried with jittered exponential backoff. On any other
// error it is rolled back and the error returned unchanged — in particular a
// lock_timeout (55P03) is NOT retried, because the lock is still held.
//
//	err := client.WithTx(ctx, &nucleus.RetryOptions{
//	    MaxAttempts:    5,
//	    IsolationLevel: "SERIALIZABLE",
//	}, func(tx *nucleus.Tx) error {
//	    _, err := tx.SQL().Exec(ctx, "UPDATE accounts SET balance = balance - 10 WHERE id = $1", id)
//	    return err
//	})
func (c *Client) WithTx(ctx context.Context, opts *RetryOptions, fn func(*Tx) error) error {
	if opts == nil {
		opts = DefaultRetryOptions()
	}
	attempts := opts.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	delay := opts.BaseDelay
	if delay <= 0 {
		delay = DefaultRetryOptions().BaseDelay
	}
	maxDelay := opts.MaxDelay
	if maxDelay <= 0 {
		maxDelay = DefaultRetryOptions().MaxDelay
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		// Honour cancellation before spending an attempt.
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("nucleus: tx retry abandoned after %d attempt(s): %w", attempt-1, lastErr)
			}
			return err
		}

		err := c.runOnce(ctx, opts.IsolationLevel, fn)
		if err == nil {
			return nil
		}
		if !IsSerializationFailure(err) {
			return err
		}
		lastErr = err

		if attempt == attempts {
			break
		}
		// Full jitter: sleep a random duration in [0, delay].
		sleep := time.Duration(rand.Int63n(int64(delay) + 1))
		select {
		case <-ctx.Done():
			return fmt.Errorf("nucleus: tx retry cancelled after %d attempt(s): %w", attempt, lastErr)
		case <-time.After(sleep):
		}
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
	return fmt.Errorf("nucleus: transaction did not succeed in %d attempt(s): %w", attempts, lastErr)
}

// runOnce executes a single attempt, guaranteeing the transaction is closed.
func (c *Client) runOnce(ctx context.Context, isolation string, fn func(*Tx) error) (err error) {
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	// A panic inside fn must not leave the transaction open holding locks —
	// on the disk engine an abandoned exclusive lock blocks every other
	// serializable transaction on that table until the session is dropped.
	committed := false
	defer func() {
		if !committed {
			// Rollback on a already-finished tx is harmless; the error is
			// deliberately discarded so it cannot mask the real one.
			_ = tx.Rollback(ctx)
		}
	}()

	if isolation != "" {
		if _, err := tx.SQL().Exec(ctx, "SET TRANSACTION ISOLATION LEVEL "+isolation); err != nil {
			return err
		}
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}
