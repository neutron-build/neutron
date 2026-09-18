package nucleus

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Snapshot-lease behavior that only a live engine can prove (Consumer-2):
// point-in-time consistency across tables under concurrent writes, the
// mutation-blocking window, conflict errors for second holders, and expiry
// recovery.
//
// Run with a live Nucleus:
//
//	NEUTRON_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55599/nucleus \
//	    go test ./nucleus/ -run SnapshotLease -v

func dropLeaseTables(t *testing.T, c *Client) {
	t.Helper()
	ctx := context.Background()
	_, _ = c.pool.Exec(ctx, "DROP TABLE IF EXISTS lease_a")
	_, _ = c.pool.Exec(ctx, "DROP TABLE IF EXISTS lease_b")
}

func TestSnapshotLeasePointInTimeUnderConcurrentWrites(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	dropLeaseTables(t, c)

	if _, err := c.pool.Exec(ctx, "CREATE TABLE lease_a (id INT)"); err != nil {
		t.Fatalf("create lease_a: %v", err)
	}
	if _, err := c.pool.Exec(ctx, "CREATE TABLE lease_b (id INT)"); err != nil {
		t.Fatalf("create lease_b: %v", err)
	}
	if _, err := c.pool.Exec(ctx, "INSERT INTO lease_a VALUES (1)"); err != nil {
		t.Fatalf("seed lease_a: %v", err)
	}
	if _, err := c.pool.Exec(ctx, "INSERT INTO lease_b VALUES (1)"); err != nil {
		t.Fatalf("seed lease_b: %v", err)
	}

	// Holder: one transaction, one moment.
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(context.Background())

	if err := tx.AcquireSnapshotLease(ctx, 5000); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// A concurrent writer inserts into lease_b. It must WAIT behind the
	// window — run it in the background and prove it has not landed.
	written := make(chan error, 1)
	go func() {
		_, err := c.pool.Exec(context.Background(), "INSERT INTO lease_b VALUES (2)")
		written <- err
	}()

	deadline := time.Now().Add(1 * time.Second)
	blocked := true
	for time.Now().Before(deadline) {
		var n int
		if err := c.pool.QueryRow(ctx, "SELECT COUNT(*) FROM lease_b").Scan(&n); err != nil {
			t.Fatalf("count during window: %v", err)
		}
		if n != 1 {
			blocked = false
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !blocked {
		t.Fatal("the concurrent write landed while the lease was held — the window is not mutation-blocking")
	}

	// The holder reads BOTH tables at the same pre-write moment.
	count := func(table string) int {
		t.Helper()
		type row struct {
			N int `db:"n"`
		}
		r, err := QueryOne[row](ctx, tx.SQL(), "SELECT COUNT(*) AS n FROM "+table)
		if err != nil {
			t.Fatalf("holder read %s: %v", table, err)
		}
		return r.N
	}
	if a, b := count("lease_a"), count("lease_b"); a != 1 || b != 1 {
		t.Fatalf("holder moment drifted: a=%d b=%d (want 1,1)", a, b)
	}

	// Holder writes are refused while it holds the lease.
	if _, err := tx.SQL().Exec(ctx, "INSERT INTO lease_a VALUES (99)"); err == nil {
		t.Fatal("holder write was accepted — the point-in-time view must be read-only")
	}

	// Release: the blocked writer proceeds and lands.
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit holder: %v", err)
	}
	if err := <-written; err != nil {
		t.Fatalf("blocked writer failed after release: %v", err)
	}
	var n int
	if err := c.pool.QueryRow(ctx, "SELECT COUNT(*) FROM lease_b").Scan(&n); err != nil {
		t.Fatalf("count after release: %v", err)
	}
	if n != 2 {
		t.Fatalf("released writer's row did not land: count=%d", n)
	}
}

func TestSnapshotLeaseConflictAndShow(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()

	// Make sure no lease is left over, then take one.
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(context.Background())
	if err := tx.AcquireSnapshotLease(ctx, 5000); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// A second transaction on another connection gets a typed conflict.
	tx2, err := c.Begin(ctx)
	if err != nil {
		t.Fatalf("begin 2: %v", err)
	}
	defer tx2.Rollback(context.Background())
	err = tx2.AcquireSnapshotLease(ctx, 1000)
	var held *ErrSnapshotLeaseHeld
	if !errors.As(err, &held) {
		t.Fatalf("second acquire must conflict with ErrSnapshotLeaseHeld, got %v", err)
	}
	if held.RemainingMillis <= 0 || held.RemainingMillis > 5000 {
		t.Fatalf("conflict error carries an implausible remaining window: %d ms", held.RemainingMillis)
	}

	// SHOW reports a holder.
	status, err := c.SnapshotLease(ctx)
	if err != nil {
		t.Fatalf("show: %v", err)
	}
	if !status.Held {
		t.Fatal("SHOW SNAPSHOT LEASE reported no holder while the lease was taken")
	}

	// Explicit release, then SHOW is empty and a second acquire succeeds.
	if err := tx.ReleaseSnapshotLease(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	status, err = c.SnapshotLease(ctx)
	if err != nil {
		t.Fatalf("show after release: %v", err)
	}
	if status.Held {
		t.Fatal("SHOW SNAPSHOT LEASE still reported a holder after release")
	}
	if err := tx2.AcquireSnapshotLease(ctx, 1000); err != nil {
		t.Fatalf("acquire after release failed: %v", err)
	}

	// Release by the wrong holder is a typed error.
	if err := tx.ReleaseSnapshotLease(ctx); !errors.Is(err, ErrNoSnapshotLease) {
		t.Fatalf("release without holding must be ErrNoSnapshotLease, got %v", err)
	}
}

func TestSnapshotLeaseExpiryRecovery(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	dropLeaseTables(t, c)
	if _, err := c.pool.Exec(ctx, "CREATE TABLE lease_a (id INT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Holder takes a short lease and then stalls (never commits; the
	// connection pool keeps the transaction's connection alive, so only
	// expiry can open the window).
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(context.Background())
	if err := tx.AcquireSnapshotLease(ctx, 300); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// A writer must get through once the lease expires.
	start := time.Now()
	if _, err := c.pool.Exec(ctx, "INSERT INTO lease_a VALUES (1)"); err != nil {
		t.Fatalf("write after expiry failed: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 200*time.Millisecond {
		t.Fatalf("write slipped through in %v — it should have waited for the lease window", elapsed)
	}

	// The expired lease is no longer reported.
	status, err := c.SnapshotLease(ctx)
	if err != nil {
		t.Fatalf("show: %v", err)
	}
	if status.Held {
		t.Fatal("expired lease still reported as held")
	}

	// And a fresh acquire works immediately after expiry.
	tx2, err := c.Begin(ctx)
	if err != nil {
		t.Fatalf("begin 2: %v", err)
	}
	defer tx2.Rollback(context.Background())
	if err := tx2.AcquireSnapshotLease(ctx, 1000); err != nil {
		t.Fatalf("acquire after expiry: %v", err)
	}
}
