package cmd

// M08 review-2 (absorbs M09): column order is informational in every mode.
// A column declared between existing ones is appended by PostgreSQL; the
// next db push and live migrate generate against the same document must
// plan nothing for the order instead of refusing it. Skipped unless
// NEUTRON_E2E_DATABASE_URL is set (NEUTRON_LIVE_REQUIRED=1 fails instead).

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestM08ColumnOrderLiveModes(t *testing.T) {
	if os.Getenv("NEUTRON_E2E_DATABASE_URL") == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; M08 column-order E2E skipped (set it to a disposable Postgres URL to run)")
	}
	bin := buildCLIBinary(t)
	must := func(t *testing.T, url string, args ...string) string {
		t.Helper()
		code, out := runCLIProcess(t, bin, url, args...)
		if code != 0 {
			t.Fatalf("neutron %s exited %d:\n%s", strings.Join(args, " "), code, out)
		}
		return out
	}
	// fixture: u(id, a, b) with a row, and documents that declare new
	// columns between existing ones (d1) and one more first (d2), plus a
	// swap of the existing a and b (d3).
	fixture := func(t *testing.T, label string) (dbURL string, query func(string) string, work string) {
		t.Helper()
		dbURL, fx := newM02CommandDB(t, label)
		if err := fx.Exec(context.Background(), `CREATE TABLE u (id integer PRIMARY KEY, a text, b text); INSERT INTO u VALUES (1, 'a1', 'b1')`); err != nil {
			t.Fatal(err)
		}
		work = t.TempDir()
		pulled := filepath.Join(work, "pulled.json")
		must(t, dbURL, "schema", "pull", "--out", pulled)
		m08ColumnsAs(t, pulled, filepath.Join(work, "d1.json"), "id", "a", "mid", "b")
		m08ColumnsAs(t, pulled, filepath.Join(work, "d2.json"), "id", "mid2", "a", "mid", "b")
		m08ColumnsAs(t, pulled, filepath.Join(work, "d3.json"), "id", "mid2", "b", "mid", "a")
		return dbURL, func(sql string) string { return q09Query(t, fx, sql) }, work
	}
	const cols = `SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'u'`

	// M09's exit: push, then push again unchanged.
	t.Run("DbPushTwice", func(t *testing.T) {
		dbURL, query, work := fixture(t, "m08push")
		d1, d2, d3 := filepath.Join(work, "d1.json"), filepath.Join(work, "d2.json"), filepath.Join(work, "d3.json")
		must(t, dbURL, "db", "push", "--schema", d1)
		if got := query(cols); got != "id,a,b,mid" {
			t.Fatalf("u columns %s", got)
		}
		for _, args := range [][]string{{"db", "push", "--schema", d1, "--dry-run"}, {"db", "push", "--schema", d1}} {
			out := must(t, dbURL, args...)
			if !strings.Contains(out, "Schema is already in sync") || !strings.Contains(out, "the database keeps its order") {
				t.Fatalf("an order difference alone plans nothing and is noted:\n%s", out)
			}
		}
		out := must(t, dbURL, "db", "push", "--schema", d2)
		if !strings.Contains(out, "mid2") {
			t.Fatalf("the next push adds mid2:\n%s", out)
		}
		// A swap of existing columns is informational too.
		out = must(t, dbURL, "db", "push", "--schema", d3)
		if !strings.Contains(out, "Schema is already in sync") {
			t.Fatalf("a swap alone plans nothing:\n%s", out)
		}
		if got := query(cols); got != "id,a,b,mid,mid2" {
			t.Fatalf("u columns %s", got)
		}
		if got := query(`SELECT a || b FROM u`); got != "a1b1" {
			t.Fatalf("u rows %s", got)
		}
	})

	// M09's exit: migrate generate --mode live, applied, then generated again.
	t.Run("LiveGenerate", func(t *testing.T) {
		dbURL, query, work := fixture(t, "m08live")
		mig := filepath.Join(work, "migrations")
		d1, d2 := filepath.Join(work, "d1.json"), filepath.Join(work, "d2.json")
		must(t, dbURL, "migrate", "generate", "--mode", "live", "--dir", mig, "--schema", d1, "--name", "add_mid")
		must(t, dbURL, "migrate", "--dir", mig)
		if got := query(cols); got != "id,a,b,mid" {
			t.Fatalf("u columns %s", got)
		}
		out := must(t, dbURL, "migrate", "generate", "--mode", "live", "--dir", mig, "--schema", d1, "--name", "again")
		if !strings.Contains(out, "No schema changes detected") {
			t.Fatalf("an order difference alone plans nothing:\n%s", out)
		}
		must(t, dbURL, "migrate", "generate", "--mode", "live", "--dir", mig, "--schema", d2, "--name", "add_mid2")
		ups, _ := filepath.Glob(filepath.Join(mig, "*_add_mid2.up.sql"))
		if len(ups) != 1 {
			t.Fatalf("add_mid2 migration missing: %v", ups)
		}
		if up := string(mustReadFile(t, ups[0])); !strings.Contains(up, `add column "mid2"`) || strings.Count(up, ";") != 1 {
			t.Fatalf("the next live plan adds mid2 only:\n%s", up)
		}
		must(t, dbURL, "migrate", "--dir", mig)
		if got := query(cols); got != "id,a,b,mid,mid2" {
			t.Fatalf("u columns %s", got)
		}
	})
}
