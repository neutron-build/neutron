package db

import (
	"context"
	"github.com/jackc/pgx/v5"
	"testing"
)

func TestMigrationNamespaceFrozenAfterCallbackPostgres(t *testing.T) {
	c := lockNamespaceClient(t, false)
	ctx := context.Background()
	s, err := c.LockMigrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	if err = s.EnsureMigrationTableV2(ctx); err != nil {
		t.Fatal(err)
	}
	original := s.namespace.schema
	foreign := original + "_other"
	quoted := pgx.Identifier{foreign}.Sanitize()
	if err = s.Exec(ctx, "CREATE SCHEMA "+quoted+"; CREATE TABLE "+pgx.Identifier{foreign, "_neutron_migrations"}.Sanitize()+" (LIKE "+s.namespace.table()+" INCLUDING ALL)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	function := pgx.Identifier{original, "shift_path"}.Sanitize()
	if err = s.Exec(ctx, "CREATE FUNCTION "+function+"() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_catalog.set_config('search_path','"+quoted+",pg_catalog',true); RETURN 1; END $$"); err != nil {
		t.Fatal(err)
	}
	mf := MigrationFile{Version: "001", Name: "callback", SQL: "SELECT " + function + "(); CREATE TABLE " + pgx.Identifier{original, "business"}.Sanitize() + " (id INT);"}
	if err = s.ApplyMigration(ctx, mf); err != nil {
		t.Fatal(err)
	}
	var own, other int
	if err = s.QueryRow(ctx, "SELECT count(*) FROM "+s.namespace.table()).Scan(&own); err != nil {
		t.Fatal(err)
	}
	if err = s.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{foreign, "_neutron_migrations"}.Sanitize()).Scan(&other); err != nil {
		t.Fatal(err)
	}
	if own != 1 || other != 0 {
		t.Fatalf("history redirected: own=%d other=%d", own, other)
	}
	mf.SQL = "SELECT " + function + "(); DROP TABLE " + pgx.Identifier{original, "business"}.Sanitize() + ";"
	if err = s.RevertMigration(ctx, mf); err != nil {
		t.Fatal(err)
	}
	if err = s.QueryRow(ctx, "SELECT count(*) FROM "+s.namespace.table()).Scan(&own); err != nil {
		t.Fatal(err)
	}
	if own != 0 {
		t.Fatalf("down retained own history: %d", own)
	}
}

func TestMigrationNamespaceRejectsAmbiguousHistoryPostgres(t *testing.T) {
	for _, kind := range []string{"later", "temporary", "unlogged", "view", "system"} {
		t.Run(kind, func(t *testing.T) {
			c := lockNamespaceClient(t, false)
			ctx := context.Background()
			conn, err := c.pool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var schema string
			if err = conn.QueryRow(ctx, "SELECT pg_catalog.current_schema()").Scan(&schema); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "later":
				other := schema + "_later"
				q := pgx.Identifier{other}.Sanitize()
				if _, err = conn.Exec(ctx, "CREATE SCHEMA "+q+"; CREATE TABLE "+pgx.Identifier{other, "_neutron_migrations"}.Sanitize()+"(version TEXT); SET search_path TO "+pgx.Identifier{schema}.Sanitize()+","+q+",pg_catalog"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := c.Exec(ctx, "DROP SCHEMA "+q+" CASCADE"); err != nil {
						t.Error(err)
					}
				})
			case "temporary":
				_, err = conn.Exec(ctx, "CREATE TEMP TABLE _neutron_migrations(version TEXT)")
			case "unlogged":
				_, err = conn.Exec(ctx, "CREATE UNLOGGED TABLE _neutron_migrations(version TEXT)")
			case "view":
				_, err = conn.Exec(ctx, "CREATE VIEW _neutron_migrations AS SELECT '1'::text AS version")
			case "system":
				_, err = conn.Exec(ctx, "SET search_path TO pg_catalog")
			}
			if err != nil {
				t.Fatal(err)
			}
			conn.Release()
			for _, op := range []string{"lock", "inspect", "applied", "has"} {
				var err error
				switch op {
				case "lock":
					var s *MigrationSession
					s, err = c.LockMigrations(ctx)
					if s != nil {
						s.Release()
					}
				case "inspect":
					_, err = c.InspectMigrationHistory(ctx)
				case "applied":
					_, err = c.AppliedMigrations(ctx)
				case "has":
					_, err = c.HasMigrationHistory(ctx)
				}
				if err == nil {
					t.Errorf("%s accepted %s metadata", op, kind)
				}
			}
			var count int
			if err = c.pool.QueryRow(ctx, "SELECT count(*) FROM pg_catalog.pg_locks WHERE locktype='advisory' AND pid=pg_catalog.pg_backend_pid()").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("refusal acquired lock")
			}
		})
	}
}

func TestMigrationNamespaceAdoptionAndRecordPostgres(t *testing.T) {
	c := lockNamespaceClient(t, false)
	ctx := context.Background()
	if err := c.Exec(ctx, "CREATE TABLE _neutron_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at TIMESTAMPTZ DEFAULT pg_catalog.now()); INSERT INTO _neutron_migrations(version,name) VALUES(1,'legacy')"); err != nil {
		t.Fatal(err)
	}
	s, err := c.LockMigrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	foreign := s.namespace.schema + "_adopt"
	q := pgx.Identifier{foreign}.Sanitize()
	if err = s.Exec(ctx, "CREATE SCHEMA "+q+"; CREATE TABLE "+pgx.Identifier{foreign, "_neutron_migrations"}.Sanitize()+" (version TEXT PRIMARY KEY,name TEXT,applied_at TIMESTAMPTZ,checksum TEXT,owner TEXT,format TEXT); SET search_path TO "+q+",pg_catalog"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Exec(ctx, "DROP SCHEMA "+q+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	legacy := MigrationFile{Version: "1", Name: "legacy", SQL: "SELECT 1;"}
	if _, err = s.AdoptMigrationHistory(ctx, []MigrationFile{legacy}); err != nil {
		t.Fatal(err)
	}
	shape, err := s.InspectMigrationHistory(ctx)
	if err != nil || shape != HistoryV2Text {
		t.Fatalf("shape=%v err=%v", shape, err)
	}
	next := MigrationFile{Version: "002", Name: "record", SQL: "SELECT 2;"}
	if err = s.RecordAppliedVersion(ctx, next); err != nil {
		t.Fatal(err)
	}
	rows, err := s.AppliedMigrations(ctx)
	if err != nil || len(rows) != 2 {
		t.Fatalf("records=%v err=%v", rows, err)
	}
	var count int
	if err = s.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{foreign, "_neutron_migrations"}.Sanitize()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("adoption/record redirected")
	}
	// A new public operation captures its configured namespace on one connection.
	if err = s.Exec(ctx, "SET search_path TO "+pgx.Identifier{s.namespace.schema}.Sanitize()+",pg_catalog"); err != nil {
		t.Fatal(err)
	}
	s.Release()
	if shape, err = c.InspectMigrationHistory(ctx); err != nil || shape != HistoryV2Text {
		t.Fatalf("public inspect: %v %v", shape, err)
	}
	if rows, err = c.AppliedMigrations(ctx); err != nil || len(rows) != 2 {
		t.Fatalf("public applied: %v %v", rows, err)
	}
	if has, err := c.HasMigrationHistory(ctx); err != nil || !has {
		t.Fatalf("public has: %v %v", has, err)
	}
}

func TestMigrationNamespaceIgnoresCatalogShadowsPostgres(t *testing.T) {
	c := lockNamespaceClient(t, false)
	ctx := context.Background()
	// All overloads used by capture/introspection are hostile when pg_catalog
	// appears explicitly after the application schema.
	for _, sql := range []string{
		"CREATE FUNCTION version() RETURNS TEXT LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'shadow version'; END $$",
		"CREATE FUNCTION current_schema() RETURNS name LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'shadow current_schema'; END $$",
		"CREATE FUNCTION to_regclass(text) RETURNS regclass LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'shadow regclass'; END $$",
		"CREATE FUNCTION format_type(oid,integer) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'shadow format_type'; END $$",
		"CREATE TABLE pg_namespace (nspname text,oid oid)",
		"CREATE TABLE pg_class (oid oid,relnamespace oid,relname name,relkind char,relpersistence char)",
	} {
		if err := c.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	s, err := c.LockMigrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.EnsureMigrationTableV2(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.InspectMigrationHistory(ctx); err != nil {
		t.Fatal(err)
	}
	s.Release()
	if exists, err := c.RelationExists(ctx, "", "_neutron_migrations"); err != nil || !exists {
		t.Fatalf("relation catalog: %v %v", exists, err)
	}
	if _, _, err := c.pinIndexTargetSchema(ctx, StatementPostcondition{Table: "_neutron_migrations"}); err != nil {
		t.Fatal(err)
	}
}
