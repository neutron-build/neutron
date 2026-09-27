package cmd

// Q11: down files of a plan with column renames run in reverse order
// against the renamed state, so every column they name before the rename is
// reverted must be the new name. Structural column lists (constraint and
// foreign-key columns, index key columns and INCLUDE) follow the rename;
// plans that depend on expression text written before the rename (offline,
// or when the live rename copy fails) are refused when that text may name a
// renamed column, with a fix that never compares text across the rename.
//
// Every case runs the real binary: migrate generate, migrate, migrate down.
// The oracle is the catalog's own text (pg_get_constraintdef,
// pg_get_indexdef, format_type) of a database built from the target SQL and
// of the original database, never the planner's introspection.

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// q11Catalog is the catalog's text of every column, constraint and index
// in schema app, plus the rows of every table.
const q11Catalog = `SELECT concat_ws(E'\n',
	(SELECT string_agg(format('%s.%s %s%s', c.relname, a.attname, format_type(a.atttypid, a.atttypmod), CASE WHEN a.attnotnull THEN ' not null' ELSE '' END), ', ' ORDER BY c.relname, a.attnum)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app' AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped),
	(SELECT string_agg(format('%s.%s %s', c.relname, co.conname, pg_get_constraintdef(co.oid)), ' | ' ORDER BY c.relname, co.conname)
		FROM pg_constraint co JOIN pg_class c ON c.oid = co.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app'),
	(SELECT string_agg(pg_get_indexdef(i.indexrelid), ' | ' ORDER BY i.indexrelid::regclass::text)
		FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app'))`

type q11Case struct {
	name    string
	live    []string // DDL of the database before the plan
	target  []string // DDL of the desired state (pulled as the schema document)
	data    []string
	renames []string
	lossy   bool // the plan drops a table: its rows do not come back
}

var q11Cases = []q11Case{
	// (b) a unique constraint on the renamed column changes; the table has
	// a check, so it gets a rename copy (Q10), which never covered the
	// constraint's column list.
	{name: "unique", renames: []string{"app.t.net>app.t.amount"},
		live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, CONSTRAINT t_u UNIQUE (net), CONSTRAINT t_c CHECK (net > 0))`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric, CONSTRAINT t_u UNIQUE (amount, id), CONSTRAINT t_c CHECK (amount > 0))`},
		data:   []string{`INSERT INTO app.t VALUES (1, 5)`}},
	// (b) the primary key on the renamed column changes.
	{name: "primary key", renames: []string{"app.t.net>app.t.amount"},
		live:   []string{`CREATE TABLE app.t (net int NOT NULL, id int NOT NULL, note text, CONSTRAINT t_pk PRIMARY KEY (net), CONSTRAINT t_c CHECK (note <> ''))`},
		target: []string{`CREATE TABLE app.t (amount int NOT NULL, id int NOT NULL, note text, CONSTRAINT t_pk PRIMARY KEY (amount, id), CONSTRAINT t_c CHECK (note <> ''))`},
		data:   []string{`INSERT INTO app.t VALUES (5, 1, 'x')`}},
	// (b) a foreign key changes; its own column and the column it
	// references are both renamed.
	{name: "foreign key", renames: []string{"app.p.code>app.p.ref", "app.c.pcode>app.c.pref"},
		live: []string{`CREATE TABLE app.p (id int PRIMARY KEY, code int NOT NULL, CONSTRAINT p_code UNIQUE (code))`,
			`CREATE TABLE app.c (id int PRIMARY KEY, pcode int, CONSTRAINT c_fk FOREIGN KEY (pcode) REFERENCES app.p (code))`},
		target: []string{`CREATE TABLE app.p (id int PRIMARY KEY, ref int NOT NULL, CONSTRAINT p_code UNIQUE (ref))`,
			`CREATE TABLE app.c (id int PRIMARY KEY, pref int, CONSTRAINT c_fk FOREIGN KEY (pref) REFERENCES app.p (ref) ON DELETE CASCADE)`},
		data: []string{`INSERT INTO app.p VALUES (1, 10)`, `INSERT INTO app.c VALUES (1, 10)`}},
	// (b) a foreign key of an unrenamed table onto a renamed column changes.
	{name: "foreign key onto renamed column", renames: []string{"app.p.code>app.p.ref"},
		live: []string{`CREATE TABLE app.p (id int PRIMARY KEY, code int NOT NULL, CONSTRAINT p_code UNIQUE (code))`,
			`CREATE TABLE app.c (id int PRIMARY KEY, pcode int, CONSTRAINT c_fk FOREIGN KEY (pcode) REFERENCES app.p (code))`},
		target: []string{`CREATE TABLE app.p (id int PRIMARY KEY, ref int NOT NULL, CONSTRAINT p_code UNIQUE (ref))`,
			`CREATE TABLE app.c (id int PRIMARY KEY, pcode int, CONSTRAINT c_fk FOREIGN KEY (pcode) REFERENCES app.p (ref) ON DELETE CASCADE)`},
		data: []string{`INSERT INTO app.p VALUES (1, 10)`, `INSERT INTO app.c VALUES (1, 10)`}},
	// (c) no expression anywhere, so no rename copy: plain index keys, an
	// INCLUDE list and a unique constraint change with the rename.
	{name: "expressionless table", renames: []string{`app.t.Net x>app.t.amount`},
		live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, "Net x" numeric, other int, CONSTRAINT t_u UNIQUE ("Net x"))`,
			`CREATE INDEX t_k ON app.t ("Net x")`, `CREATE INDEX t_i ON app.t (id) INCLUDE ("Net x")`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric, other int, CONSTRAINT t_u UNIQUE (amount, other))`,
			`CREATE INDEX t_k ON app.t (amount DESC, other)`, `CREATE INDEX t_i ON app.t (id, other) INCLUDE (amount)`},
		data: []string{`INSERT INTO app.t VALUES (1, 5, 1)`}},
	// (c) indexes naming the renamed column are dropped.
	{name: "dropped indexes", renames: []string{"app.t.net>app.t.amount"},
		live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, other int)`,
			`CREATE INDEX t_k ON app.t (net, other)`, `CREATE INDEX t_i ON app.t (other) INCLUDE (net)`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric, other int)`},
		data:   []string{`INSERT INTO app.t VALUES (1, 5, 1)`}},
	// A dropped table's foreign key onto a renamed column: its re-create
	// runs before the rename is reverted.
	{name: "dropped table", renames: []string{"app.p.code>app.p.ref"}, lossy: true,
		live: []string{`CREATE TABLE app.p (id int PRIMARY KEY, code int NOT NULL, CONSTRAINT p_code UNIQUE (code))`,
			`CREATE TABLE app.x (id int PRIMARY KEY, pc int, CONSTRAINT x_fk FOREIGN KEY (pc) REFERENCES app.p (code))`},
		target: []string{`CREATE TABLE app.p (id int PRIMARY KEY, ref int NOT NULL, CONSTRAINT p_code UNIQUE (ref))`},
		data:   []string{`INSERT INTO app.p VALUES (1, 10)`, `INSERT INTO app.x VALUES (1, 10)`}},
}

// q11Rows is the rows of every table in schema app, as JSON.
const q11Rows = `SELECT coalesce(string_agg(format('%s %s', c.relname, (xpath('/row/j/text()', query_to_xml(format('SELECT json_agg(t ORDER BY t::text)::text AS j FROM app.%I t', c.relname), false, true, '')))[1]::text), ' | ' ORDER BY c.relname), '')
	FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'app' AND c.relkind = 'r'`

// q11Target builds the target state in its own database, returns the
// catalog oracle and writes the pulled schema document to path.
func q11Target(t *testing.T, bin, path string, ddl []string) string {
	t.Helper()
	url, fx := newM02CommandDB(t, "q11t")
	for _, stmt := range append([]string{`CREATE SCHEMA app`}, ddl...) {
		if err := fx.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("target %q: %v", stmt, err)
		}
	}
	if code, out := runCLIProcess(t, bin, url, "schema", "pull", "--out", path); code != 0 {
		t.Fatalf("schema pull of the target failed (%d):\n%s", code, out)
	}
	return q09Query(t, fx, q11Catalog)
}

func q11RenameFlags(c q11Case) []string {
	var flags []string
	for _, r := range c.renames {
		flags = append(flags, "--rename", r)
	}
	return flags
}

// q11RoundTrip generates the rename migration, applies it, checks the
// state equals the target, reverts it and checks the state equals the
// original. The rows survive both directions unless the plan drops a
// table.
func q11RoundTrip(t *testing.T, bin, dbURL, mode, desired, mig, wantUp string, c q11Case, before, rowsBefore string, query func(string) string) {
	t.Helper()
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}
	args := append([]string{"migrate", "generate", "--mode", mode, "--schema", desired, "--dir", mig, "--name", "rn", "--allow-destructive"}, q11RenameFlags(c)...)
	if code, out := run(args...); code != 0 {
		t.Fatalf("generate failed (%d):\n%s", code, out)
	}
	up, _ := filepath.Glob(filepath.Join(mig, "*_rn.down.sql"))
	if len(up) != 1 {
		t.Fatalf("expected one rn migration, got %v", up)
	}
	down := readFile(t, up[0])
	if code, out := run("migrate", "--dir", mig, "--allow-destructive"); code != 0 {
		t.Fatalf("migrate failed (%d):\n%s", code, out)
	}
	if got := query(q11Catalog); got != wantUp {
		t.Fatalf("after the up, the catalog must equal the target's:\n got: %s\nwant: %s", got, wantUp)
	}
	if code, out := run("migrate", "down", "--dir", mig); code != 0 {
		t.Fatalf("migrate down failed (%d):\n%s\ndown file:\n%s", code, out, down)
	}
	if got := query(q11Catalog); got != before {
		t.Fatalf("after the down, the catalog must equal the original:\n got: %s\nwant: %s\ndown file:\n%s", got, before, down)
	}
	if got := query(q11Rows); got != rowsBefore && !c.lossy {
		t.Fatalf("rows changed across up and down:\n got: %s\nwant: %s", got, rowsBefore)
	}
}

// Live mode: each case's down file applies after its up and restores the
// original catalog, on every server version.
func TestQ11GenerateLiveDownReverts(t *testing.T) {
	bin := buildCLIBinary(t)
	for _, c := range q11Cases {
		t.Run(c.name, func(t *testing.T) {
			work := t.TempDir()
			desired := filepath.Join(work, "desired.json")
			wantUp := q11Target(t, bin, desired, c.target)
			dbURL, fx := newM02CommandDB(t, "q11live")
			for _, stmt := range append(append([]string{`CREATE SCHEMA app`}, c.live...), c.data...) {
				if err := fx.Exec(context.Background(), stmt); err != nil {
					t.Fatalf("%q: %v", stmt, err)
				}
			}
			query := func(sql string) string { return q09Query(t, fx, sql) }
			q11RoundTrip(t, bin, dbURL, "live", desired, filepath.Join(work, "migrations"), wantUp, c, query(q11Catalog), query(q11Rows), query)
		})
	}
}

// Snapshot mode (no catalog): a rename that no expression depends on
// plans offline, and its down file reverts structural column lists under
// the new names too.
func TestQ11GenerateSnapshotStructuralDownReverts(t *testing.T) {
	bin := buildCLIBinary(t)
	for _, c := range []q11Case{q11Cases[0], q11Cases[4], q11Cases[5], {
		name: "unrelated check", renames: []string{"app.t.net>app.t.amount"},
		live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, other int, CONSTRAINT t_u UNIQUE (net), CONSTRAINT t_o CHECK (other > 0))`,
			`CREATE INDEX t_k ON app.t (net)`, `CREATE INDEX t_i ON app.t (id) INCLUDE (net)`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric, other int, CONSTRAINT t_u UNIQUE (amount, id), CONSTRAINT t_o CHECK (other > 0))`,
			`CREATE INDEX t_k ON app.t (amount, other)`, `CREATE INDEX t_i ON app.t (id, other) INCLUDE (amount)`},
		data: []string{`INSERT INTO app.t VALUES (1, 5, 1)`}}, {
		// Expressions on other columns only: a default, a check, a generated
		// column, an expression index and a partial index.
		name: "unrelated expressions", renames: []string{"app.t.net>app.t.amount"},
		live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, other int, ts timestamptz DEFAULT now(), o2 int GENERATED ALWAYS AS (other * 2) STORED, CONSTRAINT t_o CHECK (other > 0), CONSTRAINT t_nu UNIQUE (net))`,
			`CREATE INDEX t_e ON app.t (abs(other))`, `CREATE INDEX t_p ON app.t (id) WHERE other > 1`, `CREATE INDEX t_n ON app.t (net) INCLUDE (other)`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric, other int, ts timestamptz DEFAULT now(), o2 int GENERATED ALWAYS AS (other * 2) STORED, CONSTRAINT t_o CHECK (other > 0), CONSTRAINT t_nu UNIQUE (amount))`,
			`CREATE INDEX t_e ON app.t (abs(other))`, `CREATE INDEX t_p ON app.t (id) WHERE other > 1`, `CREATE INDEX t_n ON app.t (amount) INCLUDE (other)`},
		data: []string{`INSERT INTO app.t (id, net, other) VALUES (1, 5, 3)`}}, {
		// Q11 review-1: expressions that name no renamed column change,
		// are re-created or are dropped along with the rename; their text
		// is the same before and after it.
		name: "unrelated expressions change", renames: []string{"app.t.net>app.t.amount"},
		live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, other int, CONSTRAINT t_o CHECK (other > 0))`,
			`CREATE INDEX t_e ON app.t (abs(other))`, `CREATE INDEX t_p ON app.t (id) WHERE other > 1`},
		target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric, other int, CONSTRAINT t_o CHECK (other > 1))`,
			`CREATE INDEX t_p ON app.t (id, other) WHERE other > 1`},
		data: []string{`INSERT INTO app.t VALUES (1, 5, 3)`}}} {
		if c.name == "unique" {
			// Its check names the renamed column: offline, that is refused
			// (TestQ11GenerateSnapshotRenameRefused).
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			work := t.TempDir()
			desired, base := filepath.Join(work, "desired.json"), filepath.Join(work, "base.json")
			wantUp := q11Target(t, bin, desired, c.target)
			q11Target(t, bin, base, c.live)
			dbURL, fx := newM02CommandDB(t, "q11snap")
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
			q11RoundTrip(t, bin, dbURL, "snapshot", desired, mig, wantUp, c, query(q11Catalog), query(q11Rows), query)
		})
	}
}

// Snapshot mode refuses a plan that depends on expression text written
// before the rename and naming the renamed column: a compared element
// (check, generated column, expression key, predicate), or a dropped or
// re-created element whose down statement would carry it. Nothing is written, and the named fix works offline:
// one migration without the elements, then one with the rename and the
// elements written with the new name; both down files revert.
func TestQ11GenerateSnapshotRenameRefused(t *testing.T) {
	bin := buildCLIBinary(t)
	cases := []struct {
		name         string
		live, target []string
		want         []string
		allowDestroy bool
	}{
		{name: "compared elements",
			live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED, CONSTRAINT t_c CHECK (net > 0))`,
				`CREATE INDEX t_e ON app.t (abs(net))`, `CREATE INDEX t_p ON app.t (id) WHERE net > 1`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric, gross numeric GENERATED ALWAYS AS (amount * 2) STORED, CONSTRAINT t_c CHECK (amount > 0))`,
				`CREATE INDEX t_e ON app.t (abs(amount))`, `CREATE INDEX t_p ON app.t (id) WHERE amount > 1`},
			want: []string{
				`check constraint t_c expression: "(amount > (0)::numeric)" (desired) vs "(net > (0)::numeric)" (planning-base)`,
				`column gross generation expression: "(amount * (2)::numeric)" (desired) vs "(net * (2)::numeric)" (planning-base)`,
				`index app.t_e key part: "abs(amount)" (desired) vs "abs(net)" (planning-base)`,
				`index app.t_p predicate: "(amount > (1)::numeric)" (desired) vs "(net > (1)::numeric)" (planning-base)`,
			}},
		{name: "dropped elements", allowDestroy: true,
			live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED, CONSTRAINT t_c CHECK (net > 0))`,
				`CREATE INDEX t_p ON app.t (id) WHERE net > 1`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric)`},
			want: []string{
				`check constraint t_c is dropped, and its down statement re-adds "(net > (0)::numeric)"`,
				`generated column gross is dropped, and its down statement re-adds it as "(net * (2)::numeric)"`,
				`index t_p is dropped, and its down statement re-creates it as "create index \"t_p\" on \"app\".\"t\" using btree (\"id\") where (net > (1)::numeric)"`,
			}},
		// Q11 review-1 finding 1: elements that differ for another reason
		// than their text are dropped and re-created from the live text too.
		{name: "key part count changes",
			live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric)`, `CREATE INDEX t_e ON app.t (abs(net))`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric)`, `CREATE INDEX t_e ON app.t (abs(amount), id)`},
			want:   []string{`index t_e is re-created, and its down statement re-creates it as "create index \"t_e\" on \"app\".\"t\" using btree ((abs(net)))"`}},
		{name: "predicate removed",
			live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric)`, `CREATE INDEX t_p ON app.t (id) WHERE net > 1`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric)`, `CREATE INDEX t_p ON app.t (id)`},
			want:   []string{`index t_p is re-created, and its down statement re-creates it as "create index \"t_p\" on \"app\".\"t\" using btree (\"id\") where (net > (1)::numeric)"`}},
		{name: "expression key part becomes a column",
			live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric)`, `CREATE INDEX t_x ON app.t (abs(net))`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric)`, `CREATE INDEX t_x ON app.t (amount)`},
			want:   []string{`index t_x is re-created, and its down statement re-creates it as "create index \"t_x\" on \"app\".\"t\" using btree ((abs(net)))"`}},
		{name: "check replaced by another constraint type",
			live:   []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, CONSTRAINT t_c CHECK (net > 0))`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric, CONSTRAINT t_c UNIQUE (amount))`},
			want:   []string{`check constraint t_c is replaced, and its down statement re-adds "(net > (0)::numeric)"`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			work := t.TempDir()
			desired, base, step1 := filepath.Join(work, "desired.json"), filepath.Join(work, "base.json"), filepath.Join(work, "step1.json")
			wantUp := q11Target(t, bin, desired, c.target)
			before := q11Target(t, bin, base, c.live)
			// The fix's first document: the table without the listed
			// elements, under the old name.
			q11Target(t, bin, step1, []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric)`})
			dbURL, fx := newM02CommandDB(t, "q11ref")
			mig := filepath.Join(work, "migrations")
			run := func(args ...string) (int, string) {
				t.Helper()
				return runCLIProcess(t, bin, dbURL, args...)
			}
			if code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", base, "--dir", mig, "--name", "init"); code != 0 {
				t.Fatalf("snapshot generate init failed (%d):\n%s", code, out)
			}
			if code, out := run("migrate", "--dir", mig); code != 0 {
				t.Fatalf("migrate init failed (%d):\n%s", code, out)
			}
			if err := fx.Exec(context.Background(), `INSERT INTO app.t (id, net) VALUES (1, 5)`); err != nil {
				t.Fatal(err)
			}
			rowsBefore := q09Query(t, fx, q11Rows)
			args := []string{"migrate", "generate", "--mode", "snapshot", "--schema", desired, "--dir", mig, "--name", "rn", "--rename", "app.t.net>app.t.amount"}
			if c.allowDestroy {
				args = append(args, "--allow-destructive")
			}
			code, out := run(args...)
			if code == 0 {
				t.Fatalf("snapshot planning must refuse:\n%s", out)
			}
			if !strings.Contains(out, "table app.t: the plan depends on expression text in the planning base (snapshot) that predates the rename of net to amount") ||
				!strings.Contains(out, "Plan it as two migrations instead: first generate one that keeps the column under its old name (net) and leaves out the elements listed below, without --rename") {
				t.Fatalf("the refusal must name the rename and the offline fix:\n%s", out)
			}
			for _, w := range c.want {
				if !strings.Contains(out, w) {
					t.Fatalf("the refusal must name %s:\n%s", w, out)
				}
			}
			if m, _ := filepath.Glob(filepath.Join(mig, "002_*")); len(m) != 0 {
				t.Fatalf("a refused plan writes nothing: %v", m)
			}
			if got := q09Query(t, fx, q11Catalog); got != before {
				t.Fatalf("the database must be untouched:\n got: %s\nwant: %s", got, before)
			}

			// The named fix, offline: two snapshot migrations.
			if code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", step1, "--dir", mig, "--name", "drop_elements", "--allow-destructive"); code != 0 {
				t.Fatalf("step 1 failed (%d):\n%s", code, out)
			}
			if code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", desired, "--dir", mig, "--name", "rn", "--rename", "app.t.net>app.t.amount"); code != 0 {
				t.Fatalf("step 2 failed (%d):\n%s", code, out)
			}
			if code, out := run("migrate", "--dir", mig, "--allow-destructive"); code != 0 {
				t.Fatalf("migrate failed (%d):\n%s", code, out)
			}
			if got := q09Query(t, fx, q11Catalog); got != wantUp {
				t.Fatalf("after the up, the catalog must equal the target's:\n got: %s\nwant: %s", got, wantUp)
			}
			for i := 0; i < 2; i++ {
				if code, out := run("migrate", "down", "--dir", mig); code != 0 {
					t.Fatalf("migrate down %d failed (%d):\n%s", i+1, code, out)
				}
			}
			if got := q09Query(t, fx, q11Catalog); got != before {
				t.Fatalf("after both downs, the catalog must equal the original:\n got: %s\nwant: %s", got, before)
			}
			if got := q09Query(t, fx, q11Rows); got != rowsBefore {
				t.Fatalf("rows changed across up and down:\n got: %s\nwant: %s", got, rowsBefore)
			}
		})
	}
}

// A live run whose rename copy failed (a whole-row check: the copy is
// made under another table name) refuses exactly the elements whose down
// statement would carry text naming the renamed column, whatever made
// them differ; the others plan, apply and revert (Q11 review-1).
func TestQ11GenerateLiveUnrenamedText(t *testing.T) {
	bin := buildCLIBinary(t)
	const rowCheck = `CONSTRAINT t_row CHECK (row_to_json(t.*) IS NOT NULL)`

	t.Run("elements naming the renamed column are refused", func(t *testing.T) {
		work := t.TempDir()
		desired := filepath.Join(work, "desired.json")
		q11Target(t, bin, desired, []string{
			`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric, other int, ` + rowCheck + `, CONSTRAINT t_c UNIQUE (amount))`,
			`CREATE INDEX t_e ON app.t (abs(amount), id)`, `CREATE INDEX t_p ON app.t (id)`, `CREATE INDEX t_x ON app.t (amount)`})
		dbURL, fx := newM02CommandDB(t, "q11unr")
		for _, stmt := range []string{`CREATE SCHEMA app`,
			`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, other int, ` + rowCheck + `, CONSTRAINT t_c CHECK (net > 0))`,
			`CREATE INDEX t_e ON app.t (abs(net))`, `CREATE INDEX t_p ON app.t (id) WHERE net > 1`, `CREATE INDEX t_x ON app.t (abs(net))`,
			`INSERT INTO app.t VALUES (1, 5, 3)`} {
			if err := fx.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("%q: %v", stmt, err)
			}
		}
		before := q09Query(t, fx, q11Catalog)
		mig := filepath.Join(work, "migrations")
		code, out := runCLIProcess(t, bin, dbURL, "migrate", "generate", "--mode", "live", "--schema", desired, "--dir", mig, "--name", "rn", "--allow-destructive", "--rename", "app.t.net>app.t.amount")
		if code == 0 {
			t.Fatalf("a plan whose down statements carry the old column name must be refused:\n%s", out)
		}
		for _, w := range []string{
			`table app.t: the plan depends on expression text in the database that predates the rename of net to amount`,
			`Rename the column by hand first: alter table "app"."t" rename column "net" to "amount" (PostgreSQL rewrites the expressions that reference it), then plan the remaining changes again, leaving out the rename of net to amount (the database already holds the new name; keep any other renames)`,
			`check constraint t_c is replaced, and its down statement re-adds "(net > (0)::numeric)"`,
			`index t_e is re-created, and its down statement re-creates it as "create index \"t_e\" on \"app\".\"t\" using btree ((abs(net)))"`,
			`index t_p is re-created, and its down statement re-creates it as "create index \"t_p\" on \"app\".\"t\" using btree (\"id\") where (net > (1)::numeric)"`,
			`index t_x is re-created, and its down statement re-creates it as "create index \"t_x\" on \"app\".\"t\" using btree ((abs(net)))"`,
		} {
			if !strings.Contains(out, w) {
				t.Fatalf("the refusal must name %s:\n%s", w, out)
			}
		}
		if strings.Contains(out, "t_row") || strings.Contains(out, "--rename flags") {
			t.Fatalf("the unchanged whole-row check is no element of the refusal, and the advice names no flags:\n%s", out)
		}
		if m, _ := filepath.Glob(filepath.Join(mig, "*_rn*")); len(m) != 0 {
			t.Fatalf("a refused plan writes nothing: %v", m)
		}
		if got := q09Query(t, fx, q11Catalog); got != before {
			t.Fatalf("the database must be untouched:\n got: %s\nwant: %s", got, before)
		}
	})

	t.Run("unrelated elements plan and revert", func(t *testing.T) {
		c := q11Case{name: "unrelated", renames: []string{"app.t.net>app.t.amount"},
			live: []string{`CREATE TABLE app.t (id int PRIMARY KEY, net numeric, other int, g int GENERATED ALWAYS AS (other * 2) STORED, ` + rowCheck + `, CONSTRAINT t_o CHECK (other > 0))`,
				`CREATE INDEX t_e ON app.t (abs(other))`, `CREATE INDEX t_q ON app.t (id) WHERE other > 1`},
			target: []string{`CREATE TABLE app.t (id int PRIMARY KEY, amount numeric, other int, ` + rowCheck + `, CONSTRAINT t_o CHECK (other > 1))`,
				`CREATE INDEX t_q ON app.t (id, other) WHERE other > 1`},
			data: []string{`INSERT INTO app.t (id, net, other) VALUES (1, 5, 3)`}}
		work := t.TempDir()
		desired := filepath.Join(work, "desired.json")
		wantUp := q11Target(t, bin, desired, c.target)
		dbURL, fx := newM02CommandDB(t, "q11unr")
		for _, stmt := range append(append([]string{`CREATE SCHEMA app`}, c.live...), c.data...) {
			if err := fx.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("%q: %v", stmt, err)
			}
		}
		query := func(sql string) string { return q09Query(t, fx, sql) }
		q11RoundTrip(t, bin, dbURL, "live", desired, filepath.Join(work, "migrations"), wantUp, c, query(q11Catalog), query(q11Rows), query)
	})
}
