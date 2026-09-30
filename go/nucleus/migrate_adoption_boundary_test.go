package nucleus

import (
	"context"
	"strings"
	"testing"
)

func TestAdoptionRefusalPreservesLegacyShape(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	resetMigrationTables(t, c)
	if _, err := c.pool.Exec(ctx, "CREATE TABLE _neutron_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.pool.Exec(ctx, "INSERT INTO _neutron_migrations VALUES (1,'first','deadbeef')"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AdoptMigrations(ctx, []Migration{{Version: 1, Name: "first", Up: "SELECT 1"}}); err == nil || !strings.Contains(err.Error(), "neither") {
		t.Fatalf("expected mismatch refusal: %v", err)
	}
	var n int
	if err := c.pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_name = '_neutron_migrations' AND column_name IN ('owner','format')").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("refused adoption left %d new metadata columns", n)
	}
}

// PostgreSQL's transactional DDL must survive a failure after the upgrade,
// not merely the preflight mismatch covered above.
func TestAdoptionPostgresUpdateFailureRollsBackShape(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	var version string
	if err := c.pool.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(version, "PostgreSQL") || strings.Contains(version, "Nucleus") {
		t.Skip("PostgreSQL catalog rollback contract only")
	}
	resetMigrationTables(t, c)
	if _, err := c.pool.Exec(ctx, "CREATE TABLE _neutron_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.pool.Exec(ctx, "INSERT INTO _neutron_migrations VALUES (1,'first')"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.pool.Exec(ctx, `CREATE OR REPLACE FUNCTION finish_adoption_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected adoption update failure'; END $$`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = c.pool.Exec(ctx, "DROP FUNCTION finish_adoption_refuse() CASCADE") })
	if _, err := c.pool.Exec(ctx, "CREATE TRIGGER finish_adoption_refuse BEFORE UPDATE ON _neutron_migrations FOR EACH ROW EXECUTE FUNCTION finish_adoption_refuse()"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AdoptMigrations(ctx, []Migration{{Version: 1, Name: "first", Up: "SELECT 1"}}); err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("expected update failure: %v", err)
	}
	var n int
	if err := c.pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_name = '_neutron_migrations' AND column_name IN ('checksum','owner','format')").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("failed adoption left %d metadata columns", n)
	}
}
