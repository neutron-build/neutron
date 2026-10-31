package nucleus

import (
	"context"
	"strings"
	"testing"
)

func TestMigrationHistoryAdmissionMissingPlan(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	for _, shape := range []string{"legacy-columns", "null-format", "unknown-format"} {
		for _, api := range []string{"up", "down", "status"} {
			t.Run(shape+"/"+api, func(t *testing.T) {
				resetMigrationTables(t, c)
				ddl := "CREATE TABLE _neutron_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at TIMESTAMPTZ DEFAULT NOW()"
				if shape != "legacy-columns" {
					ddl += ",checksum TEXT,owner TEXT,format TEXT"
				}
				ddl += ")"
				if _, err := c.pool.Exec(ctx, ddl); err != nil {
					t.Fatal(err)
				}
				if _, err := c.pool.Exec(ctx, "INSERT INTO _neutron_migrations(version,name) VALUES(1,'outside-plan')"); err != nil {
					t.Fatal(err)
				}
				if shape == "unknown-format" {
					if _, err := c.pool.Exec(ctx, "UPDATE _neutron_migrations SET format='v999'"); err != nil {
						t.Fatal(err)
					}
				}
				plan := []Migration{{Version: 2, Name: "pending", Up: "CREATE TABLE mig_b(id INT)", Down: "DROP TABLE mig_b"}}
				var err error
				switch api {
				case "up":
					err = c.Migrate(ctx, plan)
				case "down":
					err = c.MigrateDown(ctx, plan, 1)
				case "status":
					_, err = c.MigrationStatus(ctx)
				}
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), "adopt") {
					t.Errorf("%s accepted foreign history: %v", api, err)
				}
				var exists bool
				if e := c.pool.QueryRow(ctx, "SELECT to_regclass('mig_b') IS NOT NULL").Scan(&exists); e != nil {
					t.Fatal(e)
				}
				if exists {
					t.Error("pending DDL executed")
				}
				if shape == "legacy-columns" {
					var count int
					if e := c.pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='_neutron_migrations' AND column_name IN ('checksum','owner','format')").Scan(&count); e != nil {
						t.Fatal(e)
					}
					if count != 0 {
						t.Errorf("ordinary admission added %d legacy metadata columns", count)
					}
				}
			})
		}
	}
}

func TestVerifyHistoryAllRows(t *testing.T) {
	v2 := "v2"
	bad := "v999"
	for _, format := range []*string{nil, &bad} {
		if err := verifyHistory([]Migration{{Version: 2, Name: "pending", Up: "SELECT 2"}}, map[int]appliedVersion{1: {format: format}}); err == nil {
			t.Fatal("history outside plan accepted")
		}
	}
	if err := verifyHistory(nil, map[int]appliedVersion{1: {format: &v2}}); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationHistoryAdmissionCurrentSchemaAndUnverified(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	resetMigrationTables(t, c)
	if _, err := c.pool.Exec(ctx, "CREATE SCHEMA admission_decoy"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = c.pool.Exec(ctx, "DROP SCHEMA admission_decoy CASCADE") })
	if _, err := c.pool.Exec(ctx, "CREATE TABLE admission_decoy._neutron_migrations(version TEXT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.pool.Exec(ctx, migrationsTable); err != nil {
		t.Fatal(err)
	}
	if _, err := c.pool.Exec(ctx, "INSERT INTO _neutron_migrations(version,name,format) VALUES(1,'unverified','v2')"); err != nil {
		t.Fatal(err)
	}
	plan := []Migration{{Version: 2, Name: "pending", Up: "CREATE TABLE mig_b(id INT)", Down: "DROP TABLE mig_b"}}
	if err := c.Migrate(ctx, plan); err != nil {
		t.Fatal(err)
	}
	var checksum *string
	if err := c.pool.QueryRow(ctx, "SELECT checksum FROM _neutron_migrations WHERE version=1").Scan(&checksum); err != nil {
		t.Fatal(err)
	}
	if checksum != nil {
		t.Fatal("unverified checksum fabricated")
	}
	if _, err := c.MigrationStatus(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.MigrateDown(ctx, plan, 1); err != nil {
		t.Fatal(err)
	}
}
