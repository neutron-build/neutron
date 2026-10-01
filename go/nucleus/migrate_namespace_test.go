package nucleus

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func namespaceFixture(t *testing.T) (*Client, *pgx.Conn, string, string) {
	t.Helper()
	dsn := os.Getenv("NEUTRON_NAMESPACE_DATABASE_URL")
	if dsn == "" {
		t.Skip("namespace PostgreSQL URL absent")
	}
	ctx := context.Background()
	oracle, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { oracle.Close(ctx) })
	schema := fmt.Sprintf("v10_namespace_%d", time.Now().UnixNano())
	other := schema + "_other"
	for _, s := range []string{schema, other} {
		if _, err := oracle.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{s}.Sanitize()); err != nil {
			t.Fatal(err)
		}
		s := s
		t.Cleanup(func() {
			if _, err := oracle.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{s}.Sanitize()+" CASCADE"); err != nil {
				t.Error(err)
			}
		})
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	c, err := Connect(ctx, dsn, WithPoolConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, oracle, schema, other
}

func TestMigrationNamespaceRefusesBeforeMutation(t *testing.T) {
	for _, kind := range []string{"temp-history", "temp-claim", "later-history", "later-claim", "view", "temp-schema", "empty-schema"} {
		t.Run(kind, func(t *testing.T) {
			c, o, a, b := namespaceFixture(t)
			ctx := context.Background()
			stmt := ""
			switch kind {
			case "temp-history":
				stmt = "CREATE TEMP TABLE _neutron_migrations(version text)"
			case "temp-claim":
				stmt = "CREATE TEMP TABLE _neutron_migration_lock(id integer)"
			case "later-history":
				_, err := o.Exec(ctx, "CREATE TABLE "+b+"._neutron_migrations(version integer)")
				if err != nil {
					t.Fatal(err)
				}
				stmt = "SET search_path TO " + a + "," + b
			case "later-claim":
				_, err := o.Exec(ctx, "CREATE TABLE "+b+"._neutron_migration_lock(id integer)")
				if err != nil {
					t.Fatal(err)
				}
				stmt = "SET search_path TO " + a + "," + b
			case "view":
				stmt = "CREATE VIEW _neutron_migrations AS SELECT 1 AS version"
			case "temp-schema":
				stmt = "CREATE TEMP TABLE marker(id integer);SET search_path TO pg_temp"
			case "empty-schema":
				stmt = "SET search_path TO missing_namespace"
			}
			if _, err := c.Pool().Exec(ctx, stmt); err != nil {
				t.Fatal(err)
			}
			plan := []Migration{{Version: 1, Name: "pending", Up: "CREATE TABLE " + a + ".effect(id integer)", Down: "DROP TABLE " + a + ".effect"}}
			for _, api := range []string{"up", "down", "adopt", "status", "info", "unlock"} {
				var err error
				switch api {
				case "up":
					err = c.Migrate(ctx, plan)
				case "down":
					err = c.MigrateDown(ctx, plan, 1)
				case "adopt":
					_, err = c.AdoptMigrations(ctx, plan)
				case "status":
					_, err = c.MigrationStatus(ctx)
				case "info":
					_, err = c.MigrationLockInfo(ctx)
				case "unlock":
					err = c.ForceUnlockMigrations(ctx)
				}
				if err == nil {
					t.Fatalf("%s accepted %s", api, kind)
				}
			}
			var count int
			if err := o.QueryRow(ctx, "SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname IN ('effect','_neutron_migration_lock')", a).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("metadata/application mutation count=%d", count)
			}
		})
	}
}

func TestMigrationNamespaceLocalAndQualifiedLifecycle(t *testing.T) {
	c, o, a, b := namespaceFixture(t)
	ctx := context.Background()
	_, err := o.Exec(ctx, "CREATE TABLE "+b+"._neutron_migrations(version text)")
	if err != nil {
		t.Fatal(err)
	}
	plan := []Migration{{Version: 1, Name: "local", Up: "SET LOCAL search_path TO " + b + ";CREATE TABLE " + a + ".effect(id integer)", Down: "SET LOCAL search_path TO " + b + ";DROP TABLE " + a + ".effect"}}
	if err := c.Migrate(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if err := c.Migrate(ctx, plan); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := o.QueryRow(ctx, "SELECT count(*) FROM "+a+"._neutron_migrations").Scan(&n); err != nil || n != 1 {
		t.Fatalf("history %d %v", n, err)
	}
	status, err := c.MigrationStatus(ctx)
	if err != nil || len(status) != 1 {
		t.Fatalf("status %v %v", status, err)
	}
	changed := append([]Migration(nil), plan...)
	changed[0].Up += ";SELECT 1"
	if err := c.Migrate(ctx, changed); err == nil {
		t.Fatal("checksum drift accepted")
	}
	if err := c.MigrateDown(ctx, plan, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Exec(ctx, "INSERT INTO "+a+"._neutron_migration_lock(id,token,owner)VALUES(1,17,'owner')"); err != nil {
		t.Fatal(err)
	}
	info, err := c.MigrationLockInfo(ctx)
	if err != nil || info.Owner != "owner" {
		t.Fatalf("info %v %v", info, err)
	}
	if err := c.ForceUnlockMigrations(ctx); err != nil {
		t.Fatal(err)
	}
	if err := o.QueryRow(ctx, "SELECT count(*) FROM "+a+"._neutron_migration_lock").Scan(&n); err != nil || n != 0 {
		t.Fatalf("claim cleanup %d %v", n, err)
	}
}

func TestMigrationNamespacePoolChangesFrozen(t *testing.T) {
	c, o, a, b := namespaceFixture(t)
	ctx := context.Background()
	if err := c.Migrate(ctx, []Migration{{Version: 1, Name: "a", Up: "SELECT 1"}}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if _, err := o.Exec(ctx, "CREATE TABLE "+b+"._neutron_migrations(version text)"); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("NEUTRON_NAMESPACE_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 2
	var active atomic.Bool
	var seq atomic.Int32
	cfg.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
		if active.Load() {
			s := a
			if seq.Add(1)%2 == 0 {
				s = b
			}
			_, err := conn.Exec(ctx, "SET search_path TO "+s)
			return err == nil
		}
		return true
	}
	client, err := Connect(ctx, "", WithPoolConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	active.Store(true)
	records, err := client.MigrationStatus(ctx)
	if err != nil || len(records) != 1 || records[0].Name != "a" {
		t.Fatalf("frozen status %v %v", records, err)
	}
}

func TestMigrationNamespaceQuotedAdoption(t *testing.T) {
	c, o, a, _ := namespaceFixture(t)
	ctx := context.Background()
	name := a + `_neutron_migrations"select`
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := o.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := o.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
		if err != nil {
			t.Error(err)
		}
	})
	if _, err := c.Pool().Exec(ctx, "SET search_path TO "+quoted+";CREATE TABLE _neutron_migrations(version integer primary key,name text,applied_at timestamptz default now());INSERT INTO _neutron_migrations(version,name)VALUES(1,'legacy')"); err != nil {
		t.Fatal(err)
	}
	report, err := c.AdoptMigrations(ctx, nil)
	if err != nil || len(report.Unverified) != 1 {
		t.Fatalf("adoption %v %v", report, err)
	}
	records, err := c.MigrationStatus(ctx)
	if err != nil || len(records) != 1 {
		t.Fatalf("status %v %v", records, err)
	}
	if err := c.Migrate(ctx, []Migration{{Version: 2, Name: "new", Up: "SELECT 2"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Exec(ctx, "DROP TABLE "+quoted+"._neutron_migrations;CREATE TABLE "+quoted+"._neutron_migrations(version text)"); err != nil {
		t.Fatal(err)
	}
	if err := c.Migrate(ctx, []Migration{{Version: 3, Name: "no", Up: "SELECT 3"}}); err == nil || !strings.Contains(err.Error(), "text column") {
		t.Fatalf("canonical refusal %v", err)
	}
}

func TestMigrationNamespaceFunctionShadow(t *testing.T) {
	c, o, a, b := namespaceFixture(t)
	ctx := context.Background()
	if _, err := o.Exec(ctx, "CREATE FUNCTION "+a+".current_schema() RETURNS name LANGUAGE sql AS $$ SELECT '"+b+"'::name $$"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pool().Exec(ctx, "SET search_path TO "+a+",pg_catalog"); err != nil {
		t.Fatal(err)
	}
	var fake string
	if err := c.Pool().QueryRow(ctx, "SELECT current_schema()::text").Scan(&fake); err != nil || fake != b {
		t.Fatalf("shadow not active: %s %v", fake, err)
	}
	if err := c.Migrate(ctx, []Migration{{Version: 1, Name: "original", Up: "SELECT 1"}}); err != nil {
		t.Fatal(err)
	}
	records, err := c.MigrationStatus(ctx)
	if err != nil || len(records) != 1 || records[0].Name != "original" {
		t.Fatalf("status %v %v", records, err)
	}
	var n int
	if err := o.QueryRow(ctx, "SELECT count(*) FROM "+a+"._neutron_migrations").Scan(&n); err != nil || n != 1 {
		t.Fatalf("original history %d %v", n, err)
	}
	if err := o.QueryRow(ctx, "SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname IN ('_neutron_migrations','_neutron_migration_lock')", b).Scan(&n); err != nil || n != 0 {
		t.Fatalf("spoof schema mutation %d %v", n, err)
	}
}
