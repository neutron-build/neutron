package studio

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// q11StudioCatalog is the catalog's own text of every column, constraint and
// index in schema app.
const q11StudioCatalog = `SELECT concat_ws(E'\n',
	(SELECT string_agg(format('%s.%s %s', c.relname, a.attname, format_type(a.atttypid, a.atttypmod)), ', ' ORDER BY c.relname, a.attnum)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app' AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped),
	(SELECT string_agg(format('%s.%s %s', c.relname, co.conname, pg_get_constraintdef(co.oid)), ' | ' ORDER BY c.relname, co.conname)
		FROM pg_constraint co JOIN pg_class c ON c.oid = co.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app'),
	(SELECT string_agg(pg_get_indexdef(i.indexrelid), ' | ' ORDER BY i.indexrelid::regclass::text)
		FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app'))`

// TestStudioQ11RenameWithUnrelatedDropsE2E: the designer renames a column of
// a table whose rename copy fails (a whole-row check), together with drops
// of elements whose text names no renamed column. The planner plans them,
// and the plan's up and reversed down both apply. An element it cannot
// tell about is refused with advice a Studio user can follow: no CLI flags.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server; NEUTRON_LIVE_REQUIRED=1 turns a missing URL into a failure. Uses
// uniquely named q11s_* databases, dropped afterwards.
func TestStudioQ11RenameWithUnrelatedDropsE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio rename plan e2e skipped")
	}
	ctx := context.Background()
	admin, err := db.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)

	const table = `CREATE TABLE app.t (id int PRIMARY KEY, net numeric, other int, g int GENERATED ALWAYS AS (other * 2) STORED, CONSTRAINT t_row CHECK (row_to_json(t.*) IS NOT NULL))`
	rename := SchemaChange{Op: "rename-column", Schema: "app", Table: "t", From: "net", To: "amount"}
	cases := []struct {
		name    string
		ddl     []string
		changes []SchemaChange
		refused []string // refusal must contain these; empty = the plan applies and reverts
	}{
		{name: "drop expression index on another column",
			ddl:     []string{table, `CREATE INDEX t_e ON app.t (abs(other))`},
			changes: []SchemaChange{rename, {Op: "drop-index", Schema: "app", Table: "t", Index: "t_e"}}},
		{name: "drop generated column on another column",
			ddl:     []string{table},
			changes: []SchemaChange{rename, {Op: "drop-column", Schema: "app", Table: "t", Column: "g"}}},
		{name: "drop index with a whole-row predicate",
			ddl:     []string{table, `CREATE INDEX t_w ON app.t (id) WHERE t.* IS NOT NULL`},
			changes: []SchemaChange{rename, {Op: "drop-index", Schema: "app", Table: "t", Index: "t_w"}},
			refused: []string{
				`the planner refused the change set: table app.t: the plan depends on expression text in the database that predates the rename of net to amount`,
				`Rename the column by hand first: alter table "app"."t" rename column "net" to "amount" (PostgreSQL rewrites the expressions that reference it), then plan the remaining changes again, leaving out the rename of net to amount (the database already holds the new name; keep any other renames)`,
				`index t_w is dropped, and its down statement re-creates it as`,
			}},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dbName := fmt.Sprintf("q11s_%d_%d_%d", os.Getpid(), time.Now().Unix(), i)
			if err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
				t.Fatalf("create database %s: %v", dbName, err)
			}
			t.Cleanup(func() {
				cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				if err := admin.Exec(cctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName)); err != nil {
					t.Errorf("drop database %s: %v", dbName, err)
				}
			})
			dbURL := deriveStudioDatabaseURL(t, base, dbName)
			client, err := db.Connect(ctx, dbURL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			for _, stmt := range append(append([]string{`CREATE SCHEMA app`}, c.ddl...), `INSERT INTO app.t (id, net, other) VALUES (1, 5, 3)`) {
				if err := client.Exec(ctx, stmt); err != nil {
					t.Fatalf("%q: %v", stmt, err)
				}
			}
			catalog := func() string {
				t.Helper()
				rows, err := client.Query(ctx, q11StudioCatalog)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				var out string
				for rows.Next() {
					if err := rows.Scan(&out); err != nil {
						t.Fatal(err)
					}
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				return out
			}
			before := catalog()

			plan, err := PlanSchemaChanges(ctx, client, c.changes)
			if len(c.refused) > 0 {
				if err == nil {
					t.Fatalf("the planner must refuse; up:\n%s", strings.Join(plan.Up, "\n"))
				}
				for _, w := range c.refused {
					if !strings.Contains(err.Error(), w) {
						t.Fatalf("the refusal must contain %q:\n%v", w, err)
					}
				}
				if strings.Contains(err.Error(), "--rename") {
					t.Fatalf("Studio has no --rename flag; the advice must not name one:\n%v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("the change set must plan: %v", err)
			}
			apply := func(stmts []string) error {
				conn, err := pgx.Connect(ctx, dbURL)
				if err != nil {
					return err
				}
				defer conn.Close(ctx)
				tx, err := conn.Begin(ctx)
				if err != nil {
					return err
				}
				for _, s := range stmts {
					if !db.HasExecutableSQL(s) {
						continue
					}
					if _, err := tx.Exec(ctx, s); err != nil {
						_ = tx.Rollback(ctx)
						return fmt.Errorf("%s: %w", s, err)
					}
				}
				return tx.Commit(ctx)
			}
			if err := apply(plan.Up); err != nil {
				t.Fatalf("up: %v\nup:\n%s", err, strings.Join(plan.Up, "\n"))
			}
			if !strings.Contains(catalog(), "t.amount numeric") {
				t.Fatalf("the up renames the column:\n%s", catalog())
			}
			down := make([]string, 0, len(plan.Down))
			for i := len(plan.Down) - 1; i >= 0; i-- {
				down = append(down, plan.Down[i])
			}
			if err := apply(down); err != nil {
				t.Fatalf("down: %v\ndown:\n%s", err, strings.Join(down, "\n"))
			}
			if got := catalog(); got != before {
				t.Fatalf("after the down, the catalog must equal the original:\n got: %s\nwant: %s", got, before)
			}
		})
	}
}
