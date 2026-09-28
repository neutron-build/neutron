package cmd

// Q12: plans that used to fail at apply (and roll back) are ordered so
// their up applies and their down file reverts, or refused at plan time
// with the working way out named:
//
//  1. a new table whose foreign key references a column renamed in the
//     same plan (42703): the key is added after the rename;
//  2. dropping a column with a stored generated column that reads it
//     (2BP01): generated columns drop first;
//  3. changing the type of a column a generated column reads (0A000): the
//     generated column drops before the change and is added back after it,
//     with its indexes and constraints;
//  4. changing the type of a column a view the plan leaves in place reads
//     (0A000): refused, naming the view and the ways out;
//  5. the down file of a plan that drops a column with an index on it
//     (42703): the index drops first, so its down runs after the column is
//     back;
//  6. the down file of a plan that drops tables in a foreign-key cycle
//     (42P01): the re-created tables leave the cycle keys to their own down
//     statements;
//  7. changing a column's type to an enum while a check compares it with
//     text (42883): constraints and indexes drop before the type changes.
//
// Every case runs the real binary: migrate generate (live, and snapshot
// where the shape plans offline), migrate, migrate down, and db push. The
// oracle is the catalog's own text of a database built from the target
// DDL and of the original database.

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// q12CatalogSQL is q11Catalog plus views and enum labels; columns are
// listed in attnum order, or by name for a plan that moves a column last.
func q12CatalogSQL(byName bool) string {
	order := "a.attnum"
	if byName {
		order = "a.attname"
	}
	return fmt.Sprintf(`SELECT concat_ws(E'\n',
	(SELECT string_agg(format('%%s.%%s %%s%%s', c.relname, a.attname, format_type(a.atttypid, a.atttypmod), CASE WHEN a.attnotnull THEN ' not null' ELSE '' END), ', ' ORDER BY c.relname, %s)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app' AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped),
	(SELECT string_agg(format('%%s.%%s %%s', c.relname, co.conname, pg_get_constraintdef(co.oid)), ' | ' ORDER BY c.relname, co.conname)
		FROM pg_constraint co JOIN pg_class c ON c.oid = co.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app' AND co.contype <> 'n'),
	(SELECT string_agg(pg_get_indexdef(i.indexrelid), ' | ' ORDER BY i.indexrelid::regclass::text)
		FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app'),
	(SELECT string_agg(format('view %%s.%%s %%s', n.nspname, c.relname, pg_get_viewdef(c.oid)), ' | ' ORDER BY n.nspname, c.relname)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname IN ('app', 'rep') AND c.relkind = 'v'),
	(SELECT string_agg(format('enum %%s %%s', t.typname, e.enumlabel), ', ' ORDER BY t.typname, e.enumsortorder)
		FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = 'app'))`, order)
}

// q12Rows is the rows of every table in schema app, as JSON with sorted
// keys (a column that moves keeps its value).
const q12Rows = `SELECT coalesce(string_agg(format('%s %s', c.relname, (xpath('/row/j/text()', query_to_xml(format('SELECT jsonb_agg(to_jsonb(t) ORDER BY t::text)::text AS j FROM app.%I t', c.relname), false, true, '')))[1]::text), ' | ' ORDER BY c.relname), '')
	FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'app' AND c.relkind = 'r'`

type q12Case struct {
	name        string
	live        []string // DDL of the database before the plan (schema app exists)
	target      []string // DDL of the desired state (pulled as the schema document)
	data        []string
	renames     []string
	destructive bool     // generate needs --allow-destructive
	lossy       bool     // rows do not survive the round trip
	byName      bool     // the plan moves a column last: compare columns by name
	noSnapshot  bool     // offline planning refuses the shape (Q11 rename rule)
	unmanaged   []string // schemas built in the target but left out of the schema document
}

var q12Cases = []q12Case{
	// Shape 1: a new table's foreign key onto a column renamed in the plan.
	{name: "new table references a renamed column", renames: []string{"app.p.code>app.p.ref"},
		live: []string{`CREATE TABLE app.p (id int PRIMARY KEY, code int NOT NULL, CONSTRAINT p_code UNIQUE (code))`},
		target: []string{`CREATE TABLE app.p (id int PRIMARY KEY, ref int NOT NULL, CONSTRAINT p_code UNIQUE (ref))`,
			`CREATE TABLE app.n (id int PRIMARY KEY, pr int, CONSTRAINT n_fk FOREIGN KEY (pr) REFERENCES app.p (ref))`},
		data: []string{`INSERT INTO app.p VALUES (1, 10)`}},
	// Shape 1, a composite key where one column is renamed and the other
	// changes type.
	{name: "new table references a renamed and retyped key",
		renames: []string{"app.p.code>app.p.ref"},
		live:    []string{`CREATE TABLE app.p (id int PRIMARY KEY, code int NOT NULL, k int NOT NULL, CONSTRAINT p_code UNIQUE (code, k))`},
		target: []string{`CREATE TABLE app.p (id int PRIMARY KEY, ref int NOT NULL, k bigint NOT NULL, CONSTRAINT p_code UNIQUE (ref, k))`,
			`CREATE TABLE app.n (id int PRIMARY KEY, pr int, pk bigint, CONSTRAINT n_fk FOREIGN KEY (pr, pk) REFERENCES app.p (ref, k))`},
		data: []string{`INSERT INTO app.p VALUES (1, 10, 3)`}},
	// Shape 2: a column dropped with the generated column that reads it.
	{name: "drop a column and its generated column", destructive: true, lossy: true,
		live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED)`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY)`},
		data:   []string{`INSERT INTO app.t (id, net) VALUES (1, 5)`}},
	// Shape 3: the type of a column a generated column reads changes; the
	// generated column has a check, an index and a partial index.
	{name: "type change under a generated column", destructive: true,
		live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED, CONSTRAINT t_g CHECK (gross >= 0))`,
			`CREATE INDEX t_gi ON app.t (gross)`, `CREATE INDEX t_gp ON app.t (id) WHERE gross > 1`, `CREATE INDEX t_n ON app.t (net)`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net bigint, gross numeric GENERATED ALWAYS AS (net * 2) STORED, CONSTRAINT t_g CHECK (gross >= 0))`,
			`CREATE INDEX t_gi ON app.t (gross)`, `CREATE INDEX t_gp ON app.t (id) WHERE gross > 1`, `CREATE INDEX t_n ON app.t (net)`},
		data: []string{`INSERT INTO app.t (id, net) VALUES (1, 5)`}},
	// Shape 3, the generated column is not last: it moves last.
	{name: "type change under a generated column in the middle", destructive: true, byName: true,
		live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED, note text)`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net bigint, gross numeric GENERATED ALWAYS AS (net * 2) STORED, note text)`},
		data:   []string{`INSERT INTO app.t (id, net, note) VALUES (1, 5, 'x')`}},
	// Shape 3, with a rename of the column that changes type (Q10 review-1
	// F4); offline, the generated column's text names the old column.
	{name: "rename and type change under a generated column", destructive: true, noSnapshot: true,
		renames: []string{"app.t.net>app.t.amount"},
		live:    []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED)`},
		target:  []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount bigint, gross numeric GENERATED ALWAYS AS (amount * 2) STORED)`},
		data:    []string{`INSERT INTO app.t (id, net) VALUES (1, 5)`}},
	// Shape 3, the step R03 review-4 found failing: a generated column is
	// added in the plan that changes the type of the column it reads.
	{name: "generated column added over a type change",
		live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric)`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net bigint, gross numeric GENERATED ALWAYS AS (net * 2) STORED)`},
		data:   []string{`INSERT INTO app.t (id, net) VALUES (1, 5)`}},
	// Shape 4's first way out: the view is declared, so it is dropped and
	// re-created around the change.
	{name: "type change under a declared view",
		live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep int)`, `CREATE VIEW app.v AS SELECT id, keep FROM app.t`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep bigint)`, `CREATE VIEW app.v AS SELECT id, keep FROM app.t`},
		data:   []string{`INSERT INTO app.t VALUES (1, 5)`}},
	// Shape 4's second way out: --allow-destructive drops the undeclared
	// view; the down file re-creates it.
	{name: "type change under a dropped view", destructive: true,
		live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep int)`, `CREATE VIEW app.v AS SELECT id, keep FROM app.t`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep bigint)`},
		data:   []string{`INSERT INTO app.t VALUES (1, 5)`}},
	// Shape 5: a column dropped with an index on it.
	{name: "drop a column and its index", destructive: true, lossy: true,
		live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, other int, old int)`, `CREATE INDEX t_old_idx ON app.t (old)`, `CREATE INDEX t_ox ON app.t (other) INCLUDE (old)`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, other int)`},
		data:   []string{`INSERT INTO app.t VALUES (1, 2, 3)`}},
	// Shape 6: tables in a foreign-key cycle dropped, with a third table
	// that references the cycle.
	{name: "drop tables in a foreign-key cycle", destructive: true, lossy: true,
		live: []string{`CREATE TABLE app.keep (id int PRIMARY KEY)`,
			`CREATE TABLE app.x (id int PRIMARY KEY, yid int)`,
			`CREATE TABLE app.y (id int PRIMARY KEY, xid int, CONSTRAINT y_x FOREIGN KEY (xid) REFERENCES app.x (id))`,
			`ALTER TABLE app.x ADD CONSTRAINT x_y FOREIGN KEY (yid) REFERENCES app.y (id)`,
			`CREATE TABLE app.z (id int PRIMARY KEY, xid int, CONSTRAINT z_x FOREIGN KEY (xid) REFERENCES app.x (id))`},
		target: []string{`CREATE TABLE app.keep (id int PRIMARY KEY)`},
		data:   []string{`INSERT INTO app.keep VALUES (1)`, `INSERT INTO app.x VALUES (1, NULL)`, `INSERT INTO app.y VALUES (1, 1)`, `INSERT INTO app.z VALUES (1, 1)`}},
	// Shape 7: a type change to an enum while a check and a partial
	// expression index compare the column with text.
	{name: "type change to an enum under a text check",
		live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, size text, CONSTRAINT t_sz CHECK (size <> 'xl'))`,
			`CREATE INDEX t_sz_idx ON app.t (upper(size)) WHERE size <> 'x'`},
		target: []string{`CREATE TYPE app.size AS ENUM ('s', 'm', 'l', 'xl', 'x')`,
			`CREATE TABLE app.t (id int PRIMARY KEY, size app.size, CONSTRAINT t_sz CHECK (size <> 'xl'))`,
			`CREATE INDEX t_sz_idx ON app.t (size) WHERE size <> 'x'`},
		data: []string{`INSERT INTO app.t VALUES (1, 'm')`}},
	// Q12 review-1 F1: a rebuilt generated column carries a key that a
	// foreign key references, from a table sorting before it (c02), after
	// it (c03), and from itself (c25). Foreign keys drop before any key and
	// are added after every key.
	{name: "rebuilt key referenced from a table sorting before", destructive: true,
		live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED, CONSTRAINT t_g UNIQUE (gross))`,
			`CREATE TABLE app.a (id int PRIMARY KEY, g numeric, CONSTRAINT a_g FOREIGN KEY (g) REFERENCES app.t (gross))`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net bigint, gross numeric GENERATED ALWAYS AS (net * 2) STORED, CONSTRAINT t_g UNIQUE (gross))`,
			`CREATE TABLE app.a (id int PRIMARY KEY, g numeric, CONSTRAINT a_g FOREIGN KEY (g) REFERENCES app.t (gross))`},
		data: []string{`INSERT INTO app.t (id, net) VALUES (1, 5)`, `INSERT INTO app.a VALUES (1, 10)`}},
	{name: "rebuilt key referenced from a table sorting after", destructive: true,
		live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED, CONSTRAINT t_g UNIQUE (gross))`,
			`CREATE TABLE app.z (id int PRIMARY KEY, g numeric, CONSTRAINT z_g FOREIGN KEY (g) REFERENCES app.t (gross))`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net bigint, gross numeric GENERATED ALWAYS AS (net * 2) STORED, CONSTRAINT t_g UNIQUE (gross))`,
			`CREATE TABLE app.z (id int PRIMARY KEY, g numeric, CONSTRAINT z_g FOREIGN KEY (g) REFERENCES app.t (gross))`},
		data: []string{`INSERT INTO app.t (id, net) VALUES (1, 5)`, `INSERT INTO app.z VALUES (1, 10)`}},
	{name: "rebuilt primary key with a self-referencing foreign key", destructive: true, byName: true,
		live:   []string{`CREATE TABLE app.t (net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED, parent numeric, CONSTRAINT t_pk PRIMARY KEY (gross), CONSTRAINT t_parent FOREIGN KEY (parent) REFERENCES app.t (gross))`},
		target: []string{`CREATE TABLE app.t (net bigint, gross numeric GENERATED ALWAYS AS (net * 2) STORED, parent numeric, CONSTRAINT t_pk PRIMARY KEY (gross), CONSTRAINT t_parent FOREIGN KEY (parent) REFERENCES app.t (gross))`},
		data:   []string{`INSERT INTO app.t (net, parent) VALUES (5, NULL)`, `INSERT INTO app.t (net, parent) VALUES (6, 10)`}},
	// The three cross-table siblings that failed before Q12 too (review-1
	// c07, c08, c13).
	{name: "drop a foreign key and the unique it references",
		live: []string{`CREATE TABLE app.a (id int PRIMARY KEY, code int, CONSTRAINT a_code UNIQUE (code))`,
			`CREATE TABLE app.z (id int PRIMARY KEY, c int, CONSTRAINT z_c FOREIGN KEY (c) REFERENCES app.a (code))`},
		target: []string{`CREATE TABLE app.a (id int PRIMARY KEY, code int)`, `CREATE TABLE app.z (id int PRIMARY KEY, c int)`},
		data:   []string{`INSERT INTO app.a VALUES (1, 7)`, `INSERT INTO app.z VALUES (1, 7)`}},
	{name: "add a unique and a foreign key onto it",
		live: []string{`CREATE TABLE app.a (id int PRIMARY KEY, c int)`, `CREATE TABLE app.z (id int PRIMARY KEY, code int)`},
		target: []string{`CREATE TABLE app.z (id int PRIMARY KEY, code int, CONSTRAINT z_code UNIQUE (code))`,
			`CREATE TABLE app.a (id int PRIMARY KEY, c int, CONSTRAINT a_c FOREIGN KEY (c) REFERENCES app.z (code))`},
		data: []string{`INSERT INTO app.z VALUES (1, 7)`, `INSERT INTO app.a VALUES (1, 7)`}},
	{name: "widen a primary key and the foreign key onto it",
		live: []string{`CREATE TABLE app.p (id int, k int, CONSTRAINT p_pkey PRIMARY KEY (id))`, `CREATE TABLE app.c (id int PRIMARY KEY, pid int, CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES app.p (id))`},
		target: []string{`CREATE TABLE app.p (id int, k int NOT NULL, CONSTRAINT p_pkey PRIMARY KEY (id, k))`,
			`CREATE TABLE app.c (id int PRIMARY KEY, pid int, pk int, CONSTRAINT c_fk FOREIGN KEY (pid, pk) REFERENCES app.p (id, k))`},
		data: []string{`INSERT INTO app.p VALUES (1, 1)`, `INSERT INTO app.c VALUES (1, 1)`}},
	// An unchanged foreign key onto a key that is replaced (here renamed):
	// it is dropped and re-added around the key.
	{name: "unchanged foreign key onto a changed key",
		live: []string{`CREATE TABLE app.p (id int PRIMARY KEY, code int, CONSTRAINT p_code UNIQUE (code))`,
			`CREATE TABLE app.a (id int PRIMARY KEY, c int, CONSTRAINT a_c FOREIGN KEY (c) REFERENCES app.p (code))`},
		target: []string{`CREATE TABLE app.p (id int PRIMARY KEY, code int, CONSTRAINT p_code_key UNIQUE (code))`,
			`CREATE TABLE app.a (id int PRIMARY KEY, c int, CONSTRAINT a_c FOREIGN KEY (c) REFERENCES app.p (code))`},
		data: []string{`INSERT INTO app.p VALUES (1, 7)`, `INSERT INTO app.a VALUES (1, 7)`}},
	// Q12 review-1 F5: a new table's foreign key onto a column (c20) or a
	// unique (c21) the plan adds to an existing table.
	{name: "new table references a column the plan adds",
		live: []string{`CREATE TABLE app.p (id int PRIMARY KEY)`},
		target: []string{`CREATE TABLE app.p (id int PRIMARY KEY, code int, CONSTRAINT p_code UNIQUE (code))`,
			`CREATE TABLE app.n (id int PRIMARY KEY, c int, CONSTRAINT n_c FOREIGN KEY (c) REFERENCES app.p (code))`},
		data: []string{`INSERT INTO app.p VALUES (1)`}},
	{name: "new table references a unique the plan adds",
		live: []string{`CREATE TABLE app.p (id int PRIMARY KEY, code int)`},
		target: []string{`CREATE TABLE app.p (id int PRIMARY KEY, code int, CONSTRAINT p_code UNIQUE (code))`,
			`CREATE TABLE app.n (id int PRIMARY KEY, c int, CONSTRAINT n_c FOREIGN KEY (c) REFERENCES app.p (code))`},
		data: []string{`INSERT INTO app.p VALUES (1, 1)`}},
	// Q12 review-1 F3: a view over a same-named table and column in
	// another schema does not block the change (live: the catalog knows
	// it reads rep.t). Offline, the text check is conservative and
	// refuses it, so there is no snapshot leg.
	{name: "view over a same-named column of another schema", noSnapshot: true, unmanaged: []string{"rep"},
		live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep int)`, `CREATE SCHEMA rep`, `CREATE TABLE rep.t (id int, keep int)`, `CREATE VIEW rep.v AS SELECT keep FROM rep.t`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep bigint)`, `CREATE SCHEMA rep`, `CREATE TABLE rep.t (id int, keep int)`, `CREATE VIEW rep.v AS SELECT keep FROM rep.t`},
		data:   []string{`INSERT INTO app.t VALUES (1, 2)`}},
}

// q12Pulled caches pulled documents and catalogs by their DDL: the three
// flows share them, so each target database is built once per run.
var q12Pulled = struct {
	sync.Mutex
	m map[string][2]string
}{m: map[string][2]string{}}

// q12Target builds DDL in its own database, writes the pulled schema
// document to path and returns the catalog oracle.
func q12Target(t *testing.T, bin, path string, ddl []string, byName bool) string {
	t.Helper()
	key := fmt.Sprintf("%v %q", byName, ddl)
	q12Pulled.Lock()
	defer q12Pulled.Unlock()
	if got, ok := q12Pulled.m[key]; ok {
		writeFile(t, path, got[0])
		return got[1]
	}
	url, fx := newM02CommandDB(t, "q12t")
	for _, stmt := range append([]string{`CREATE SCHEMA app`}, ddl...) {
		if err := fx.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("target %q: %v", stmt, err)
		}
	}
	if code, out := runCLIProcess(t, bin, url, "schema", "pull", "--out", path); code != 0 {
		t.Fatalf("schema pull failed (%d):\n%s", code, out)
	}
	catalog := q09Query(t, fx, q12CatalogSQL(byName))
	q12Pulled.m[key] = [2]string{readFile(t, path), catalog}
	return catalog
}

// q12Unmanage removes schemas (and their tables, views and enums) from a
// pulled schema document: the target database has them, the document does
// not manage them.
func q12Unmanage(t *testing.T, path string, schemas []string) {
	t.Helper()
	if len(schemas) == 0 {
		return
	}
	drop := map[string]bool{}
	for _, s := range schemas {
		drop[s] = true
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(readFile(t, path)), &doc); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"schemas", "tables", "views", "enums", "opaque"} {
		list, _ := doc[key].([]any)
		kept := []any{}
		for _, e := range list {
			m, _ := e.(map[string]any)
			name, _ := m["name"].(string)
			if id, ok := m["identity"].(map[string]any); ok {
				name, _ = id["schema"].(string)
			}
			if !drop[name] {
				kept = append(kept, e)
			}
		}
		doc[key] = kept
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, string(raw))
}

func q12Flags(c q12Case) []string {
	var flags []string
	for _, r := range c.renames {
		flags = append(flags, "--rename", r)
	}
	if c.destructive {
		flags = append(flags, "--allow-destructive")
	}
	return flags
}

// q12Seed creates the live state and its rows in a fresh database.
func q12Seed(t *testing.T, fx *db.Client, c q12Case) {
	t.Helper()
	for _, stmt := range append(append([]string{`CREATE SCHEMA app`}, c.live...), c.data...) {
		if err := fx.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("%q: %v", stmt, err)
		}
	}
}

// q12RoundTrip generates the migration, applies it, checks the catalog
// equals the target's, reverts it and checks the catalog equals the
// original. Rows survive both directions unless the plan drops data.
func q12RoundTrip(t *testing.T, bin, dbURL, mode, desired, mig, wantUp, before, rowsBefore string, c q12Case, query func(string) string) {
	t.Helper()
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}
	args := append([]string{"migrate", "generate", "--mode", mode, "--schema", desired, "--dir", mig, "--name", "q12"}, q12Flags(c)...)
	code, out := run(args...)
	if code != 0 {
		t.Fatalf("generate failed (%d):\n%s", code, out)
	}
	t.Logf("generate (%s):\n%s", mode, out)
	files, _ := filepath.Glob(filepath.Join(mig, "*_q12.*.sql"))
	if len(files) != 2 {
		t.Fatalf("expected one q12 migration (up and down), got %v", files)
	}
	up, down := readFile(t, files[1]), readFile(t, files[0])
	t.Logf("up:\n%s\ndown:\n%s", up, down)
	if code, out := run("migrate", "--dir", mig, "--allow-destructive"); code != 0 {
		t.Fatalf("migrate failed (%d):\n%s\nup file:\n%s", code, out, up)
	}
	catalog := q12CatalogSQL(c.byName)
	if got := query(catalog); got != wantUp {
		t.Fatalf("after the up, the catalog must equal the target's:\n got: %s\nwant: %s", got, wantUp)
	}
	if code, out := run("migrate", "down", "--dir", mig); code != 0 {
		t.Fatalf("migrate down failed (%d):\n%s\ndown file:\n%s", code, out, down)
	}
	if got := query(catalog); got != before {
		t.Fatalf("after the down, the catalog must equal the original:\n got: %s\nwant: %s\ndown file:\n%s", got, before, down)
	}
	if got := query(q12Rows); got != rowsBefore && !c.lossy {
		t.Fatalf("rows changed across up and down:\n got: %s\nwant: %s", got, rowsBefore)
	}
}

// Live mode: every shape's up applies and its down file restores the
// original catalog.
func TestQ12GenerateLiveRoundTrip(t *testing.T) {
	bin := buildCLIBinary(t)
	for _, c := range q12Cases {
		t.Run(c.name, func(t *testing.T) {
			work := t.TempDir()
			desired := filepath.Join(work, "desired.json")
			wantUp := q12Target(t, bin, desired, c.target, c.byName)
			q12Unmanage(t, desired, c.unmanaged)
			dbURL, fx := newM02CommandDB(t, "q12live")
			q12Seed(t, fx, c)
			query := func(sql string) string { return q09Query(t, fx, sql) }
			q12RoundTrip(t, bin, dbURL, "live", desired, filepath.Join(work, "migrations"), wantUp, query(q12CatalogSQL(c.byName)), query(q12Rows), c, query)
		})
	}
}

// Snapshot mode: the same shapes plan offline against the chain's
// recorded state.
func TestQ12GenerateSnapshotRoundTrip(t *testing.T) {
	bin := buildCLIBinary(t)
	for _, c := range q12Cases {
		if c.noSnapshot {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			work := t.TempDir()
			desired, base := filepath.Join(work, "desired.json"), filepath.Join(work, "base.json")
			wantUp := q12Target(t, bin, desired, c.target, c.byName)
			q12Target(t, bin, base, c.live, c.byName)
			dbURL, fx := newM02CommandDB(t, "q12snap")
			mig := filepath.Join(work, "migrations")
			if code, out := runCLIProcess(t, bin, dbURL, "migrate", "generate", "--mode", "snapshot", "--schema", base, "--dir", mig, "--name", "init"); code != 0 {
				t.Fatalf("snapshot generate init failed (%d):\n%s", code, out)
			}
			if code, out := runCLIProcess(t, bin, dbURL, "migrate", "--dir", mig); code != 0 {
				t.Fatalf("migrate init failed (%d):\n%s", code, out)
			}
			for _, stmt := range c.data {
				if err := fx.Exec(context.Background(), stmt); err != nil {
					t.Fatal(err)
				}
			}
			query := func(sql string) string { return q09Query(t, fx, sql) }
			q12RoundTrip(t, bin, dbURL, "snapshot", desired, mig, wantUp, query(q12CatalogSQL(c.byName)), query(q12Rows), c, query)
		})
	}
}

// db push: every shape applies in one transaction, and a second plan is in
// sync.
func TestQ12DBPush(t *testing.T) {
	bin := buildCLIBinary(t)
	for _, c := range q12Cases {
		t.Run(c.name, func(t *testing.T) {
			work := t.TempDir()
			desired := filepath.Join(work, "desired.json")
			wantUp := q12Target(t, bin, desired, c.target, c.byName)
			q12Unmanage(t, desired, c.unmanaged)
			dbURL, fx := newM02CommandDB(t, "q12push")
			q12Seed(t, fx, c)
			args := append([]string{"db", "push", "--schema", desired}, q12Flags(c)...)
			if code, out := runCLIProcess(t, bin, dbURL, args...); code != 0 {
				t.Fatalf("db push failed (%d):\n%s", code, out)
			}
			if got := q09Query(t, fx, q12CatalogSQL(c.byName)); got != wantUp {
				t.Fatalf("after the push, the catalog must equal the target's:\n got: %s\nwant: %s", got, wantUp)
			}
			code, out := runCLIProcess(t, bin, dbURL, "db", "push", "--dry-run", "--schema", desired)
			inSync := strings.Contains(out, "Schema is already in sync") ||
				len(c.unmanaged) > 0 && strings.Contains(out, "No applicable changes") && !strings.Contains(out, "alter table")
			if code != 0 || !inSync {
				t.Fatalf("a second plan must be in sync (%d):\n%s", code, out)
			}
		})
	}
}

// Refused at plan time, with the way out named: nothing is written or
// applied, in every flow.
func TestQ12Refusals(t *testing.T) {
	bin := buildCLIBinary(t)
	cases := []struct {
		name         string
		live, target []string
		flags        []string
		want         []string
		snapshot     bool
	}{
		// Shape 4: a view the schema does not declare reads the column.
		{name: "undeclared view over a type change", snapshot: true,
			live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep int, other int)`, `CREATE VIEW app.v AS SELECT id, keep FROM app.t`, `CREATE VIEW app.w AS SELECT id, other FROM app.t`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep bigint, other int)`},
			want: []string{
				`view app.v is not declared in the schema, so the plan leaves it in place, but `,
				`column app.t.keep`,
				`so the plan would fail at apply and is refused. Declare the view in the schema (the plan then drops it and re-creates it around the change), or drop it: re-run with --allow-destructive, which drops views the schema does not declare`,
				// F2: the refusal names everything else the flag drops.
				`Note that --allow-destructive also drops every object the schema does not declare, which here is: view app.v, view app.w; declare in the schema what must stay before using it`,
			}},
		// Shape 4, a view outside the managed schemas.
		{name: "view in an unmanaged schema over a type change",
			live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep int)`, `CREATE SCHEMA rep`, `CREATE VIEW rep.v AS SELECT id, keep FROM app.t`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep bigint)`},
			flags:  []string{"--allow-destructive"},
			want: []string{
				`view rep.v is in schema "rep", which the schema document does not manage, so the plan leaves it in place, but it uses column app.t.keep (type changes).`,
				`Drop it by hand before applying and re-create it after, or declare schema "rep" and the view in the schema`,
			}},
		// Shape 4 for a dropped column (2BP01).
		{name: "undeclared view over a dropped column",
			live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep int, old int)`, `CREATE SCHEMA rep`, `CREATE VIEW rep.v AS SELECT id, old FROM app.t`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep int)`},
			flags:  []string{"--allow-destructive"},
			want:   []string{`view rep.v is in schema "rep", which the schema document does not manage, so the plan leaves it in place, but it uses column app.t.old (is dropped).`}},
		// Shape 3 needs the destructive acknowledgement.
		{name: "type change under a generated column without acknowledgement", snapshot: true,
			live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED)`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net bigint, gross numeric GENERATED ALWAYS AS (net * 2) STORED)`},
			want:   []string{`table app.t: generated column "gross" reads column "net", whose type changes. PostgreSQL cannot change the type of a column a generated column reads, so the plan drops "gross" before the change and adds it back after it: its stored values are recomputed, it is placed last in the table, and privileges or comments on it are not kept (the down file adds it back last as well). Re-run with --allow-destructive to acknowledge that (with this schema, --allow-destructive drops nothing else)`}},
		// Q12 review-1 F2: the refusal names what else the flag drops.
		{name: "type change under a generated column names what the flag also drops", snapshot: true,
			live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED, legacy text)`,
				`CREATE TABLE app.audit (id int PRIMARY KEY, msg text)`, `CREATE INDEX t_legacy ON app.t (legacy)`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net bigint, gross numeric GENERATED ALWAYS AS (net * 2) STORED, legacy text)`},
			want:   []string{`Re-run with --allow-destructive to acknowledge that. Note that --allow-destructive also drops every object the schema does not declare, which here is: index app.t_legacy, table app.audit; declare in the schema what must stay before using it`}},
		// Q12 review-1 F4: dependencies the view text does not show, read
		// from the catalog (live only; offline plans have no catalog).
		{name: "view over a function returning the table's rows",
			live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep int, other int)`,
				`CREATE FUNCTION app.f() RETURNS SETOF app.t LANGUAGE sql AS 'select * from app.t'`, `CREATE VIEW app.v AS SELECT * FROM app.f()`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep bigint, other int)`},
			want:   []string{`view app.v is not declared in the schema, so the plan leaves it in place, but it uses column app.t.keep (type changes).`}},
		{name: "unmanaged view over a declared view",
			live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep int)`, `CREATE VIEW app.v AS SELECT id, keep FROM app.t`,
				`CREATE SCHEMA rep`, `CREATE VIEW rep.w AS SELECT id, keep FROM app.v`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep bigint)`, `CREATE VIEW app.v AS SELECT id, keep FROM app.t`},
			want:   []string{`view app.v is dropped by this plan (to re-create it around the table changes, or because the schema does not declare it), but view rep.w depends on it.`, `Drop it by hand before applying and re-create it after, or declare schema "rep" and the view in the schema`}},
		{name: "BEGIN ATOMIC function over the column",
			live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep int)`, `CREATE SCHEMA rep`,
				`CREATE FUNCTION rep.g(n int) RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT keep FROM app.t WHERE id = n; END`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep bigint)`},
			want:   []string{`column app.t.keep (type changes) is used by function rep.g(integer), which the schema does not describe.`, `Drop it by hand before applying and re-create it after`}},
		{name: "materialized view and row-type column",
			live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep int)`, `CREATE SCHEMA rep`,
				`CREATE MATERIALIZED VIEW rep.m AS SELECT keep FROM app.t`, `CREATE TABLE rep.u (x app.t)`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, keep bigint)`},
			want: []string{`column app.t.keep (type changes) is used by materialized view rep.m, which the schema does not describe.`,
				`the row type of table app.t (a column of it changes type) is used by column rep.u.x (it stores the row type of app.t)`}},
		// Shape 3 with a foreign key the plan does not manage onto the
		// generated column.
		{name: "type change under a referenced generated column",
			live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED, CONSTRAINT t_g UNIQUE (gross))`,
				`CREATE SCHEMA rep`, `CREATE TABLE rep.r (id int PRIMARY KEY, g numeric, CONSTRAINT r_g FOREIGN KEY (g) REFERENCES app.t (gross))`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net bigint, gross numeric GENERATED ALWAYS AS (net * 2) STORED, CONSTRAINT t_g UNIQUE (gross))`},
			flags:  []string{"--allow-destructive"},
			want:   []string{`table app.t: generated column "gross" must be dropped and added back (a column it reads changes type), but foreign key r_g of table rep.r, which the schema does not manage here, references it. Plan it as three migrations: remove the column (and that foreign key) from the schema, then change the type, then add the column back`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			work := t.TempDir()
			desired, base := filepath.Join(work, "desired.json"), filepath.Join(work, "base.json")
			q12Target(t, bin, desired, c.target, false)
			check := func(flow string, code int, out string) {
				t.Helper()
				if code == 0 {
					t.Fatalf("%s must refuse:\n%s", flow, out)
				}
				for _, w := range c.want {
					if !strings.Contains(out, w) {
						t.Fatalf("%s: the refusal must say %q:\n%s", flow, w, out)
					}
				}
			}
			dbURL, fx := newM02CommandDB(t, "q12ref")
			q12Seed(t, fx, q12Case{live: c.live})
			before := q09Query(t, fx, q12CatalogSQL(false))
			mig := filepath.Join(work, "migrations")
			code, out := runCLIProcess(t, bin, dbURL, append([]string{"migrate", "generate", "--mode", "live", "--schema", desired, "--dir", mig, "--name", "q12"}, c.flags...)...)
			check("migrate generate --mode live", code, out)
			if m, _ := filepath.Glob(filepath.Join(mig, "*_q12.*")); len(m) != 0 {
				t.Fatalf("a refused plan writes nothing: %v", m)
			}
			code, out = runCLIProcess(t, bin, dbURL, append([]string{"db", "push", "--schema", desired}, c.flags...)...)
			check("db push", code, out)
			if got := q09Query(t, fx, q12CatalogSQL(false)); got != before {
				t.Fatalf("the database must be untouched:\n got: %s\nwant: %s", got, before)
			}
			if !c.snapshot {
				return
			}
			q12Target(t, bin, base, c.live, false)
			snapURL, _ := newM02CommandDB(t, "q12refsnap")
			smig := filepath.Join(work, "snapmig")
			if code, out := runCLIProcess(t, bin, snapURL, "migrate", "generate", "--mode", "snapshot", "--schema", base, "--dir", smig, "--name", "init"); code != 0 {
				t.Fatalf("snapshot generate init failed (%d):\n%s", code, out)
			}
			code, out = runCLIProcess(t, bin, snapURL, append([]string{"migrate", "generate", "--mode", "snapshot", "--schema", desired, "--dir", smig, "--name", "q12"}, c.flags...)...)
			check("migrate generate --mode snapshot", code, out)
			if m, _ := filepath.Glob(filepath.Join(smig, "*_q12.*")); len(m) != 0 {
				t.Fatalf("a refused plan writes nothing: %v", m)
			}
		})
	}
}
