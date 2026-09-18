package nucleus

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Snapshot lease (Consumer-2): a cross-table point-in-time boundary with a
// mutation-blocking window, driven over plain SQL. While the lease is held,
// other connections' writes wait at the engine's dispatch gate until the
// lease is released or expires, so a backup can read related tables one by
// one — even on separate connections — without mixing logical moments.
//
// The lease is scoped to a transaction: AcquireSnapshotLease must run
// inside a Tx (the point-in-time view IS that transaction's snapshot), and
// the engine releases the lease automatically at COMMIT/ROLLBACK, on
// connection loss, or at the timeout.

// ErrNoSnapshotLease is returned by ReleaseSnapshotLease when the
// transaction does not hold the lease.
var ErrNoSnapshotLease = errors.New("nucleus: this transaction does not hold the snapshot lease")

// ErrSnapshotLeaseHeld reports that another session holds the lease.
// RemainingMillis is how much of that holder's window is left.
type ErrSnapshotLeaseHeld struct {
	RemainingMillis int64
}

func (e *ErrSnapshotLeaseHeld) Error() string {
	return fmt.Sprintf("nucleus: a snapshot lease is already held by another session (%d ms remaining)", e.RemainingMillis)
}

// AcquireSnapshotLease takes the database-wide snapshot lease for this
// transaction. timeoutMillis bounds how long a crashed holder can block
// writers (blocked writes wake at the latest at expiry); 0 means the
// engine default (30s). The lease is released at COMMIT/ROLLBACK, on
// connection close, or at expiry — ReleaseSnapshotLease releases it early.
//
// The holder's transaction is the point-in-time view: its reads keep one
// logical moment across every table, and its own writes are refused while
// it holds the lease.
func (t *Tx) AcquireSnapshotLease(ctx context.Context, timeoutMillis int64) error {
	sql := "ACQUIRE SNAPSHOT LEASE"
	if timeoutMillis > 0 {
		sql = "ACQUIRE SNAPSHOT LEASE TIMEOUT " + strconv.FormatInt(timeoutMillis, 10)
	}
	if _, err := t.tx.Exec(ctx, sql); err != nil {
		if held := leaseHeldFrom(err); held != nil {
			return held
		}
		return fmt.Errorf("nucleus: acquire snapshot lease: %w", err)
	}
	return nil
}

// ReleaseSnapshotLease releases the lease early. Optional: COMMIT,
// ROLLBACK, and connection close all release it.
func (t *Tx) ReleaseSnapshotLease(ctx context.Context) error {
	if _, err := t.tx.Exec(ctx, "RELEASE SNAPSHOT LEASE"); err != nil {
		if strings.Contains(err.Error(), "does not hold the lease") {
			return ErrNoSnapshotLease
		}
		return fmt.Errorf("nucleus: release snapshot lease: %w", err)
	}
	return nil
}

// SnapshotLeaseStatus describes the current lease holder, if any.
type SnapshotLeaseStatus struct {
	Held            bool
	HolderSessionID int64
	RemainingMillis int64
}

// SnapshotLease reports the engine's current lease state (SHOW SNAPSHOT
// LEASE). Works on any connection; not transaction-scoped.
func (c *Client) SnapshotLease(ctx context.Context) (SnapshotLeaseStatus, error) {
	var status SnapshotLeaseStatus
	rows, err := c.pool.Query(ctx, "SHOW SNAPSHOT LEASE")
	if err != nil {
		return status, fmt.Errorf("nucleus: show snapshot lease: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&status.HolderSessionID, &status.RemainingMillis); err != nil {
			return status, fmt.Errorf("nucleus: show snapshot lease: scan: %w", err)
		}
		status.Held = true
	}
	return status, rows.Err()
}

// leaseHeldFrom parses the engine's "held by another session" refusal back
// into a typed error, so callers can distinguish conflict from failure
// without string-matching themselves.
func leaseHeldFrom(err error) *ErrSnapshotLeaseHeld {
	if err == nil {
		return nil
	}
	const (
		marker = "already held by another session ("
		suffix = " ms remaining)"
	)
	msg := err.Error()
	idx := strings.Index(msg, marker)
	if idx < 0 {
		return nil
	}
	rest := msg[idx+len(marker):]
	end := strings.Index(rest, suffix)
	if end < 0 {
		return nil
	}
	ms, parseErr := strconv.ParseInt(rest[:end], 10, 64)
	if parseErr != nil {
		return nil
	}
	return &ErrSnapshotLeaseHeld{RemainingMillis: ms}
}
