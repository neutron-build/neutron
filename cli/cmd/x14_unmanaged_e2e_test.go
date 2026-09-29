package cmd

// X14: a table the schema document declares with managed: false is not
// neutron's. The real binary never creates, alters or drops it under
// --allow-destructive (db push, migrate generate live and snapshot), does
// not report drift on it (schema check --live, the drift gate before
// migrate), lets a managed table reference it, and schema pull keeps the
// marker of the document it overwrites.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func x14Exec(t *testing.T, fx interface {
	Exec(context.Context, string, ...any) error
}, stmts ...string) {
	t.Helper()
	for _, stmt := range stmts {
		if err := fx.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("%q: %v", stmt, err)
		}
	}
}

const x14TableCount = `SELECT count(*)::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'app' AND c.relkind = 'r' AND c.relname = '%s'`

func x14Has(t *testing.T, name string, q func(string) string) bool {
	t.Helper()
	return q(strings.Replace(x14TableCount, "%s", name, 1)) == "1"
}

func TestX14LiveDestructiveKeepsUnmanagedTable(t *testing.T) {
	bin := buildCLIBinary(t)
	work := t.TempDir()
	desired := filepath.Join(work, "desired.json")
	q12Target(t, bin, desired, []string{
		`CREATE TABLE app.t (id int PRIMARY KEY, extra int)`,
		`CREATE TABLE app.k (id int PRIMARY KEY)`,
		`CREATE TABLE app.m (id int PRIMARY KEY, kid int, CONSTRAINT m_kid_fkey FOREIGN KEY (kid) REFERENCES app.k (id))`,
	}, false)
	q12MarkUnmanaged(t, desired, []string{"app.k"})

	dbURL, fx := newM02CommandDB(t, "x14live")
	q := func(sql string) string { return q09Query(t, fx, sql) }
	x14Exec(t, fx, `CREATE SCHEMA app`, `CREATE TABLE app.t (id int PRIMARY KEY)`,
		`CREATE TABLE app.k (id int PRIMARY KEY, secret text)`, `INSERT INTO app.k VALUES (1, 'kept')`,
		`CREATE TABLE app.u (id int PRIMARY KEY)`)

	// migrate generate --mode live plans the change without touching k.
	mig := filepath.Join(work, "migrations")
	code, out := runCLIProcess(t, bin, dbURL, "migrate", "generate", "--mode", "live", "--schema", desired, "--dir", mig, "--name", "x14", "--allow-destructive")
	if code != 0 {
		t.Fatalf("generate failed (%d):\n%s", code, out)
	}
	ups, _ := filepath.Glob(filepath.Join(mig, "*_x14.up.sql"))
	if len(ups) != 1 {
		t.Fatalf("want one up file, got %v", ups)
	}
	up, err := os.ReadFile(ups[0])
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(up))
	for _, forbidden := range []string{`drop table if exists "app"."k"`, `alter table "app"."k"`, `create table "app"."k"`} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("the plan must not touch app.k (%s):\n%s", forbidden, up)
		}
	}
	if !strings.Contains(lower, `drop table if exists "app"."u"`) {
		t.Fatalf("app.u drops:\n%s", up)
	}

	code, out = runCLIProcess(t, bin, dbURL, "db", "push", "--schema", desired, "--allow-destructive")
	if code != 0 {
		t.Fatalf("db push failed (%d):\n%s", code, out)
	}
	if !x14Has(t, "k", q) || x14Has(t, "u", q) || !x14Has(t, "m", q) {
		t.Fatalf("app.k must stay, app.u must go, app.m must exist:\n%s", out)
	}
	if got := q(`SELECT string_agg(secret, ',') FROM app.k`); got != "kept" {
		t.Fatalf("app.k rows and columns are untouched, got %q", got)
	}
	if got := q(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'm_kid_fkey'`); !strings.Contains(got, "REFERENCES app.k(id)") {
		t.Fatalf("the managed foreign key onto the unmanaged table exists: %q", got)
	}
	code, out = runCLIProcess(t, bin, dbURL, "db", "push", "--schema", desired, "--allow-destructive")
	if code != 0 || !strings.Contains(out, "Schema is already in sync") {
		t.Fatalf("a second push is in sync (%d):\n%s", code, out)
	}
	if !strings.Contains(out, "declared managed: false") {
		t.Fatalf("the plan notes the unmanaged table:\n%s", out)
	}
}

func TestX14LiveRefusesForeignKeyOntoMissingUnmanagedTable(t *testing.T) {
	bin := buildCLIBinary(t)
	work := t.TempDir()
	desired := filepath.Join(work, "desired.json")
	q12Target(t, bin, desired, []string{
		`CREATE TABLE app.k (id int PRIMARY KEY)`,
		`CREATE TABLE app.m (id int PRIMARY KEY, kid int, CONSTRAINT m_kid_fkey FOREIGN KEY (kid) REFERENCES app.k (id))`,
	}, false)
	q12MarkUnmanaged(t, desired, []string{"app.k"})
	dbURL, fx := newM02CommandDB(t, "x14miss")
	q := func(sql string) string { return q09Query(t, fx, sql) }
	x14Exec(t, fx, `CREATE SCHEMA app`)
	code, out := runCLIProcess(t, bin, dbURL, "db", "push", "--schema", desired)
	if code == 0 || !strings.Contains(out, "managed: false") || !strings.Contains(out, "m_kid_fkey") {
		t.Fatalf("want a refusal naming the key and the unmanaged table (%d):\n%s", code, out)
	}
	if x14Has(t, "k", q) || x14Has(t, "m", q) {
		t.Fatalf("a refused push creates nothing:\n%s", out)
	}
}

func TestX14SnapshotChainKeepsUnmanagedTableAndNoDrift(t *testing.T) {
	bin := buildCLIBinary(t)
	work := t.TempDir()
	mig := filepath.Join(work, "migrations")
	dbURL, fx := newM02CommandDB(t, "x14snap")
	q := func(sql string) string { return q09Query(t, fx, sql) }
	x14Exec(t, fx, `CREATE SCHEMA app`, `CREATE TABLE app.t (id int PRIMARY KEY)`,
		`CREATE TABLE app.k (id int PRIMARY KEY, secret text)`, `INSERT INTO app.k VALUES (1, 'kept')`)
	if code, out := runCLIProcess(t, bin, dbURL, "schema", "baseline", "--dir", mig); code != 0 {
		t.Fatalf("baseline failed (%d):\n%s", code, out)
	}

	// Step 1: k declared managed: false (with content unlike the database's).
	first := filepath.Join(work, "first.json")
	q12Target(t, bin, first, []string{
		`CREATE TABLE app.t (id int PRIMARY KEY, extra int)`,
		`CREATE TABLE app.k (id int PRIMARY KEY)`,
	}, false)
	q12MarkUnmanaged(t, first, []string{"app.k"})
	gen := func(schema, name string) {
		t.Helper()
		code, out := runCLIProcess(t, bin, dbURL, "migrate", "generate", "--mode", "snapshot", "--schema", schema, "--dir", mig, "--name", name, "--allow-destructive")
		if code != 0 {
			t.Fatalf("snapshot generate %s failed (%d):\n%s", name, code, out)
		}
		ups, _ := filepath.Glob(filepath.Join(mig, "*_"+name+".up.sql"))
		if len(ups) != 1 {
			t.Fatalf("want one up file for %s, got %v", name, ups)
		}
		up, _ := os.ReadFile(ups[0])
		if strings.Contains(strings.ToLower(string(up)), `"app"."k"`) {
			t.Fatalf("the plan must not touch app.k:\n%s", up)
		}
	}
	apply := func() {
		t.Helper()
		if code, out := runCLIProcess(t, bin, dbURL, "migrate", "--dir", mig, "--allow-destructive"); code != 0 {
			t.Fatalf("migrate failed (%d):\n%s", code, out)
		}
	}
	gen(first, "declared")
	apply()
	if !x14Has(t, "k", q) {
		t.Fatalf("app.k survives a destructive migration")
	}

	// Changes made to k by other means are not drift.
	x14Exec(t, fx, `ALTER TABLE app.k ADD COLUMN z int`)
	if code, out := runCLIProcess(t, bin, dbURL, "schema", "check", "--live", "--schema", first, "--dir", mig); code != 0 {
		t.Fatalf("schema check --live must report no drift on an unmanaged table (%d):\n%s", code, out)
	}
	if code, out := runCLIProcess(t, bin, dbURL, "schema", "check", "--schema", first, "--dir", mig); code != 0 {
		t.Fatalf("offline schema check is in sync (%d):\n%s", code, out)
	}

	// Step 2: the document stops mentioning k; the chain still records it
	// as not neutron's, and the drift gate lets the next migration run.
	second := filepath.Join(work, "second.json")
	q12Target(t, bin, second, []string{`CREATE TABLE app.t (id int PRIMARY KEY, extra int, more int)`}, false)
	gen(second, "omitted")
	apply()
	if !x14Has(t, "k", q) || q(`SELECT count(*)::text FROM app.k`) != "1" {
		t.Fatalf("app.k and its rows survive")
	}
	if code, out := runCLIProcess(t, bin, dbURL, "schema", "check", "--live", "--schema", second, "--dir", mig); code != 0 {
		t.Fatalf("no drift after the omitted step (%d):\n%s", code, out)
	}
}

func TestX14PullKeepsUnmanagedMarker(t *testing.T) {
	bin := buildCLIBinary(t)
	work := t.TempDir()
	doc := filepath.Join(work, "schema.json")
	dbURL, fx := newM02CommandDB(t, "x14pull")
	x14Exec(t, fx, `CREATE SCHEMA app`, `CREATE TABLE app.t (id int PRIMARY KEY)`, `CREATE TABLE app.k (id int PRIMARY KEY)`)
	if code, out := runCLIProcess(t, bin, dbURL, "schema", "pull", "--out", doc); code != 0 {
		t.Fatalf("pull failed (%d):\n%s", code, out)
	}
	q12MarkUnmanaged(t, doc, []string{"app.k"})
	x14Exec(t, fx, `ALTER TABLE app.k ADD COLUMN z int`, `ALTER TABLE app.t ADD COLUMN y int`)
	code, out := runCLIProcess(t, bin, dbURL, "schema", "pull", "--out", doc)
	if code != 0 || !strings.Contains(out, "kept managed: false") || !strings.Contains(out, "table app.k") {
		t.Fatalf("pull reports the kept marker (%d):\n%s", code, out)
	}
	var parsed struct {
		Tables []struct {
			Identity struct{ Name string } `json:"identity"`
			Managed  bool                  `json:"managed"`
			Columns  []any                 `json:"columns"`
		} `json:"tables"`
	}
	if err := json.Unmarshal([]byte(readFile(t, doc)), &parsed); err != nil {
		t.Fatal(err)
	}
	for _, tb := range parsed.Tables {
		switch tb.Identity.Name {
		case "k":
			if tb.Managed || len(tb.Columns) != 2 {
				t.Fatalf("k keeps managed: false with the live content: %+v", tb)
			}
		case "t":
			if !tb.Managed || len(tb.Columns) != 2 {
				t.Fatalf("t stays managed with the live content: %+v", tb)
			}
		}
	}
}

func TestX14PullRefusesInvalidPreviousDocument(t *testing.T) {
	bin := buildCLIBinary(t)
	doc := filepath.Join(t.TempDir(), "schema.json")
	dbURL, fx := newM02CommandDB(t, "x14invalid")
	x14Exec(t, fx, `CREATE TABLE k (id int PRIMARY KEY)`)
	// A damaged document may contain ownership markers that cannot be read.
	// Pull must preserve it and refuse rather than silently taking ownership.
	previous := `{"version":2,"tables":[{"managed":false` + "\n"
	if err := os.WriteFile(doc, []byte(previous), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out := runCLIProcess(t, bin, dbURL, "schema", "pull", "--out", doc)
	if code == 0 || !strings.Contains(out, "previous schema") {
		t.Fatalf("want a refusal to read the previous schema (%d):\n%s", code, out)
	}
	if got := readFile(t, doc); got != previous {
		t.Fatalf("pull overwrote the invalid ownership document: %q", got)
	}
}
