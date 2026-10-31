package db

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
	"time"
)

func lockNamespaceClient(t *testing.T, shadow bool) *Client {
	t.Helper()
	dsn := os.Getenv("NEUTRON_VALUES_DATABASE_URL")
	if dsn == "" {
		t.Skip("PostgreSQL lock control URL absent")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close(ctx) })
	schema := fmt.Sprintf("v10_cli_lock_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	if shadow {
		for _, sql := range []string{
			"CREATE FUNCTION " + pgx.Identifier{schema, "pg_advisory_lock"}.Sanitize() + "(bigint) RETURNS void LANGUAGE SQL AS $$ SELECT pg_catalog.pg_sleep(0) $$",
			"CREATE FUNCTION " + pgx.Identifier{schema, "pg_advisory_unlock"}.Sanitize() + "(bigint) RETURNS boolean LANGUAGE SQL AS $$ SELECT true $$",
		} {
			if _, err := admin.Exec(ctx, sql); err != nil {
				t.Fatal(err)
			}
		}
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize() + ", pg_catalog"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &Client{pool: pool, url: dsn}
}

func TestMigrationLockIgnoresShadowFunctionsPostgres(t *testing.T) {
	c := lockNamespaceClient(t, true)
	s, err := c.LockMigrations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	var locks int
	if err := s.QueryRow(context.Background(), "SELECT count(*) FROM pg_catalog.pg_locks WHERE locktype='advisory' AND pid=pg_catalog.pg_backend_pid()").Scan(&locks); err != nil {
		t.Fatal(err)
	}
	if locks != 1 {
		t.Fatalf("migration session holds %d actual advisory locks, want 1", locks)
	}
	s.Release()
	if err := c.pool.QueryRow(context.Background(), "SELECT count(*) FROM pg_catalog.pg_locks WHERE locktype='advisory' AND pid=pg_catalog.pg_backend_pid()").Scan(&locks); err != nil {
		t.Fatal(err)
	}
	if locks != 0 {
		t.Fatalf("migration session retained %d advisory locks after release", locks)
	}
}

func TestMigrationUnconfirmedUnlockDiscardsSessionPostgres(t *testing.T) {
	c := lockNamespaceClient(t, false)
	s, err := c.LockMigrations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	var oldPID int32
	if err := s.QueryRow(context.Background(), "SELECT pg_catalog.pg_backend_pid()").Scan(&oldPID); err != nil {
		t.Fatal(err)
	}
	// Simulate a run whose expected lock is no longer confirmed on its session.
	var unlocked bool
	if err := s.QueryRow(context.Background(), "SELECT pg_catalog.pg_advisory_unlock($1)", migrationAdvisoryLockKey).Scan(&unlocked); err != nil || !unlocked {
		t.Fatalf("control unlock: %v %v", unlocked, err)
	}
	s.Release()
	s.Release() // Idempotent even when discard was necessary.
	var newPID int32
	if err := c.pool.QueryRow(context.Background(), "SELECT pg_catalog.pg_backend_pid()").Scan(&newPID); err != nil {
		t.Fatal(err)
	}
	if oldPID == newPID {
		t.Fatalf("unconfirmed unlock recycled backend %d", oldPID)
	}
}
