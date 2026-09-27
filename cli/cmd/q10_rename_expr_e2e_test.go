package cmd

// Q10 live coverage through the REAL CLI binary: renaming a column that a
// generated column, a check or an index references. PostgreSQL rewrites
// those expressions on RENAME COLUMN itself, so a rename alone must plan
// only the rename (no SET EXPRESSION, which PostgreSQL 16 cannot run, and
// no check or index churn), and a rename with a real change must still
// plan the change. Skipped unless NEUTRON_E2E_DATABASE_URL is set
// (NEUTRON_LIVE_REQUIRED=1 fails instead).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// q10Elements picks the expression elements of app.t that reference the
// renamed column; each field is a format string whose %[1]s is the
// column's quoted-as-needed name ("" leaves the element out).
type q10Elements struct {
	generated string // gross generated always as (...)
	check     string // check t_c
	exprIndex string // t_expr_idx on (...)
	partial   string // t_part_idx on (id) where ...
}

// q10Doc renders app.t with column col (numeric, referenced as ref in the
// expressions) plus id, label and the chosen elements.
func q10Doc(col, ref string, e q10Elements) string {
	cols := []string{
		`{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}`,
		fmt.Sprintf(`{"name": %q, "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false}`, col),
		`{"name": "label", "type": {"name": "text", "codec": "string"}, "notNull": false}`,
	}
	if e.generated != "" {
		cols = append(cols, fmt.Sprintf(`{"name": "gross", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false, "generated": {"expression": %q}}`, fmt.Sprintf(e.generated, ref)))
	}
	cons := []string{`{"name": "t_pkey", "type": "primary-key", "columns": ["id"]}`}
	if e.check != "" {
		cons = append(cons, fmt.Sprintf(`{"name": "t_c", "type": "check", "expression": %q}`, fmt.Sprintf(e.check, ref)))
	}
	var idx []string
	if e.exprIndex != "" {
		idx = append(idx, fmt.Sprintf(`{"identity": {"schema": "app", "name": "t_expr_idx"}, "unique": false, "method": "btree", "key": [{"expression": %q}]}`, fmt.Sprintf(e.exprIndex, ref)))
	}
	if e.partial != "" {
		idx = append(idx, fmt.Sprintf(`{"identity": {"schema": "app", "name": "t_part_idx"}, "unique": false, "method": "btree", "key": [{"column": "id"}], "where": %q}`, fmt.Sprintf(e.partial, ref)))
	}
	return fmt.Sprintf(`{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "app"}], "enums": [],
	"tables": [{"identity": {"schema": "app", "name": "t"}, "managed": true,
		"columns": [%s], "constraints": [%s], "indexes": [%s]}],
	"views": [], "opaque": []
}`, strings.Join(cols, ", "), strings.Join(cons, ", "), strings.Join(idx, ", "))
}

// q10Identity is the physical identity of app.t and its check and indexes:
// a table rewrite, a check re-add or an index rebuild changes it.
const q10Identity = `SELECT format('table %s; checks %s; indexes %s',
	(SELECT relfilenode FROM pg_class WHERE oid = 'app.t'::regclass),
	(SELECT coalesce(string_agg(conname || '=' || oid, ',' ORDER BY conname), '') FROM pg_constraint WHERE conrelid = 'app.t'::regclass AND contype = 'c'),
	(SELECT coalesce(string_agg(c.relname || '=' || c.relfilenode, ',' ORDER BY c.relname), '') FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = 'app.t'::regclass))`

// q10Definitions is the catalog's own text of every expression element of
// app.t, independent of the planner's introspection.
const q10Definitions = `SELECT concat_ws(' | ',
	(SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum WHERE d.adrelid = 'app.t'::regclass AND a.attname = 'gross'),
	(SELECT string_agg(pg_get_constraintdef(oid), ' | ' ORDER BY conname) FROM pg_constraint WHERE conrelid = 'app.t'::regclass AND contype = 'c'),
	(SELECT string_agg(pg_get_indexdef(indexrelid), ' | ' ORDER BY indexrelid::regclass::text) FROM pg_index WHERE indrelid = 'app.t'::regclass AND NOT indisprimary))`

// q10Statements lists the SQL statements a dry run printed.
func q10Statements(out string) []string {
	var stmts []string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasSuffix(line, ";") && !strings.HasPrefix(line, "--") {
			stmts = append(stmts, strings.TrimSuffix(line, ";"))
		}
	}
	return stmts
}

// q10FileStatements lists the statements of a generated .sql file.
func q10FileStatements(t *testing.T, path string) []string {
	t.Helper()
	var stmts []string
	for _, line := range strings.Split(readFile(t, path), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		stmts = append(stmts, strings.TrimSuffix(line, ";"))
	}
	return stmts
}

type q10Case struct {
	name     string
	old, new string // column names
	oldRef   string // how the expressions spell the old column
	newRef   string
	elems    q10Elements
	want     string // the catalog's q10Definitions after the rename
}

var q10RenameOnlyCases = []q10Case{
	{name: "generated", old: "net", new: "amount", oldRef: "net", newRef: "amount", elems: q10Elements{generated: "%s * 2"},
		want: `(amount * (2)::numeric)`},
	{name: "check", old: "net", new: "amount", oldRef: "net", newRef: "amount", elems: q10Elements{check: "%s > 0"},
		want: `CHECK ((amount > (0)::numeric))`},
	{name: "expression index", old: "net", new: "amount", oldRef: "net", newRef: "amount", elems: q10Elements{exprIndex: "abs(%s)"},
		want: `CREATE INDEX t_expr_idx ON app.t USING btree (abs(amount))`},
	{name: "partial index", old: "net", new: "amount", oldRef: "net", newRef: "amount", elems: q10Elements{partial: "%s > 1"},
		want: `CREATE INDEX t_part_idx ON app.t USING btree (id) WHERE (amount > (1)::numeric)`},
	{name: "all four", old: "net", new: "amount", oldRef: "net", newRef: "amount",
		elems: q10Elements{generated: "%s * 2", check: "%s > 0", exprIndex: "abs(%s)", partial: "%s > 1"},
		want:  `(amount * (2)::numeric) | CHECK ((amount > (0)::numeric)) | CREATE INDEX t_expr_idx ON app.t USING btree (abs(amount)) | CREATE INDEX t_part_idx ON app.t USING btree (id) WHERE (amount > (1)::numeric)`},
	// The old name inside string literals must stay as it is.
	{name: "literal", old: "net", new: "amount", oldRef: "net", newRef: "amount",
		elems: q10Elements{generated: "%s + length('net')", check: "%s > 0 and label <> 'net'", partial: "label <> 'net' and %s > 1"},
		want:  `(amount + (length('net'::text))::numeric) | CHECK (((amount > (0)::numeric) AND (label <> 'net'::text))) | CREATE INDEX t_part_idx ON app.t USING btree (id) WHERE ((label <> 'net'::text) AND (amount > (1)::numeric))`},
	// A column named like the function applied to it.
	{name: "function-named column", old: "abs", new: "val", oldRef: "abs", newRef: "val",
		elems: q10Elements{generated: "abs(%s)", check: "abs(%s) > 0", exprIndex: "abs(%s)"},
		want:  `abs(val) | CHECK ((abs(val) > (0)::numeric)) | CREATE INDEX t_expr_idx ON app.t USING btree (abs(val))`},
	{name: "quoted mixed case", old: "Net", new: "Amount", oldRef: `"Net"`, newRef: `"Amount"`,
		elems: q10Elements{generated: "%s * 2", check: "%s > 0", exprIndex: "abs(%s)", partial: "%s > 1"},
		want:  `("Amount" * (2)::numeric) | CHECK (("Amount" > (0)::numeric)) | CREATE INDEX t_expr_idx ON app.t USING btree (abs("Amount")) | CREATE INDEX t_part_idx ON app.t USING btree (id) WHERE ("Amount" > (1)::numeric)`},
}

func q10Rename(c q10Case) string {
	return "app.t." + c.old + ">app.t." + c.new
}

// A rename alone plans only RENAME COLUMN, on every server version: the
// table, its checks and its indexes keep their physical identity, and the
// catalog follows the rename by itself.
func TestQ10PushRenameOnlyRenames(t *testing.T) {
	bin := buildCLIBinary(t)
	for i, c := range q10RenameOnlyCases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			dbURL, fx := newM02CommandDB(t, "q10push"+strconv.Itoa(i))
			work := t.TempDir()
			before, after := filepath.Join(work, "before.json"), filepath.Join(work, "after.json")
			writeFile(t, before, q10Doc(c.old, c.oldRef, c.elems))
			writeFile(t, after, q10Doc(c.new, c.newRef, c.elems))
			major := q09ServerMajor(t, fx)
			run := func(args ...string) (int, string) {
				t.Helper()
				return runCLIProcess(t, bin, dbURL, args...)
			}

			if code, out := run("db", "push", "--schema", before); code != 0 {
				t.Fatalf("push before failed (%d):\n%s", code, out)
			}
			if err := fx.Exec(context.Background(), fmt.Sprintf(`INSERT INTO app.t (id, %s, label) VALUES (1, 5, 'x')`, q10Quote(c.old))); err != nil {
				t.Fatal(err)
			}
			identity := q09Query(t, fx, q10Identity)
			rows := q09Query(t, fx, `SELECT row_to_json(t)::text FROM app.t t`)

			rename := fmt.Sprintf(`alter table "app"."t" rename column %s to %s`, q10Quote(c.old), q10Quote(c.new))
			code, out := run("db", "push", "--dry-run", "--schema", after, "--rename", q10Rename(c))
			if code != 0 {
				t.Fatalf("PostgreSQL %d: a rename alone must plan (%d):\n%s", major, code, out)
			}
			if stmts := q10Statements(out); len(stmts) != 1 || stmts[0] != rename {
				t.Fatalf("PostgreSQL %d: a rename alone must plan exactly %q, got %q:\n%s", major, rename, stmts, out)
			}
			if strings.Contains(out, "equivalence not verified") || strings.Contains(out, "\n!") || strings.HasPrefix(out, "!") {
				t.Fatalf("PostgreSQL %d: a rename alone carries no warning:\n%s", major, out)
			}

			if code, out := run("db", "push", "--schema", after, "--rename", q10Rename(c)); code != 0 || !strings.Contains(out, "Pushed 1 statement(s)") {
				t.Fatalf("PostgreSQL %d: push failed (%d):\n%s", major, code, out)
			}
			if got := q09Query(t, fx, q10Identity); got != identity {
				t.Fatalf("PostgreSQL %d: no rewrite, re-add or rebuild expected: %s -> %s", major, identity, got)
			}
			// The catalog rewrote the expressions itself: column
			// references renamed, literals and function names kept.
			if got := q09Query(t, fx, q10Definitions); got != c.want {
				t.Fatalf("PostgreSQL %d: catalog definitions after the rename:\n got %s\nwant %s", major, got, c.want)
			}
			if got := q09Query(t, fx, `SELECT row_to_json(t)::text FROM app.t t`); got != strings.Replace(rows, `"`+c.old+`"`, `"`+c.new+`"`, 1) {
				t.Fatalf("PostgreSQL %d: rows changed: %s -> %s", major, rows, got)
			}
			if code, out := run("db", "push", "--dry-run", "--schema", after); code != 0 || !strings.Contains(out, "in sync") {
				t.Fatalf("PostgreSQL %d: the renamed schema must converge (%d):\n%s", major, code, out)
			}
		})
	}
}

func q10Quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// A rename combined with a real change still plans the change, and only
// the change: the untouched elements are not re-created.
func TestQ10PushRenameWithRealChange(t *testing.T) {
	bin := buildCLIBinary(t)
	all := q10Elements{generated: "%s * 2", check: "%s > 0 and label <> 'net'", exprIndex: "abs(%s)", partial: "%s > 1"}
	cases := []struct {
		name      string
		changed   q10Elements
		statement string // the change statement after the rename
		gated     bool   // SET EXPRESSION: PostgreSQL 17+
		keep      string // identity parts that must not change
	}{
		{name: "generated", changed: q10Elements{generated: "%s * 3", check: all.check, exprIndex: all.exprIndex, partial: all.partial},
			statement: `alter table "app"."t" alter column "gross" set expression as ((amount * (3)::numeric))`, gated: true},
		{name: "literal in check", changed: q10Elements{generated: all.generated, check: "%s > 0 and label <> 'amount'", exprIndex: all.exprIndex, partial: all.partial},
			statement: `alter table "app"."t" add constraint "t_c" check (((amount > (0)::numeric) AND (label <> 'amount'::text)))`},
		{name: "partial predicate", changed: q10Elements{generated: all.generated, check: all.check, exprIndex: all.exprIndex, partial: "%s > 2"},
			statement: `create index "t_part_idx" on "app"."t" using btree ("id") where (amount > (2)::numeric)`},
	}
	for i, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			dbURL, fx := newM02CommandDB(t, "q10chg"+strconv.Itoa(i))
			work := t.TempDir()
			before, after := filepath.Join(work, "before.json"), filepath.Join(work, "after.json")
			writeFile(t, before, q10Doc("net", "net", all))
			writeFile(t, after, q10Doc("amount", "amount", c.changed))
			major := q09ServerMajor(t, fx)
			run := func(args ...string) (int, string) {
				t.Helper()
				return runCLIProcess(t, bin, dbURL, args...)
			}
			if code, out := run("db", "push", "--schema", before); code != 0 {
				t.Fatalf("push before failed (%d):\n%s", code, out)
			}
			if err := fx.Exec(context.Background(), `INSERT INTO app.t (id, net, label) VALUES (1, 5, 'x')`); err != nil {
				t.Fatal(err)
			}
			identity := q09Query(t, fx, q10Identity)

			code, out := run("db", "push", "--dry-run", "--schema", after, "--rename", "app.t.net>app.t.amount")
			if c.gated && major < 17 {
				if code == 0 {
					t.Fatalf("PostgreSQL %d cannot run SET EXPRESSION; the plan must be refused:\n%s", major, out)
				}
				if !strings.Contains(out, `generated column "gross" changes its expression`) || strings.Contains(out, "could not be verified") {
					t.Fatalf("PostgreSQL %d: the refusal must call the verified change a change:\n%s", major, out)
				}
				if code, out := run("db", "push", "--schema", after, "--rename", "app.t.net>app.t.amount"); code == 0 || strings.Contains(out, "applied:") {
					t.Fatalf("PostgreSQL %d: a refused push applies nothing (%d):\n%s", major, code, out)
				}
				if got := q09Query(t, fx, q10Identity); got != identity || q09Query(t, fx, `SELECT count(*)::text FROM pg_attribute WHERE attrelid = 'app.t'::regclass AND attname = 'net'`) != "1" {
					t.Fatalf("PostgreSQL %d: the refused plan must leave the table as it was", major)
				}
				return
			}
			if code != 0 {
				t.Fatalf("PostgreSQL %d: dry run failed (%d):\n%s", major, code, out)
			}
			stmts := q10Statements(out)
			if len(stmts) == 0 || stmts[0] != `alter table "app"."t" rename column "net" to "amount"` {
				t.Fatalf("the plan must start with the rename:\n%s", out)
			}
			found := false
			for _, s := range stmts[1:] {
				if s == c.statement {
					found = true
				}
			}
			if !found {
				t.Fatalf("PostgreSQL %d: the real change %q must be planned:\n%s", major, c.statement, out)
			}
			// Only the changed element is touched: gross, t_c and the two
			// indexes each appear in the plan only when they changed.
			for _, elem := range []string{`"gross"`, `"t_c"`, `"t_expr_idx"`, `"t_part_idx"`} {
				touched := false
				for _, s := range stmts[1:] {
					if strings.Contains(s, elem) {
						touched = true
					}
				}
				if touched != strings.Contains(c.statement, elem) {
					t.Fatalf("PostgreSQL %d: %s touched=%v, but only the changed element may be:\n%s", major, elem, touched, out)
				}
			}
			if strings.Contains(out, "equivalence not verified") {
				t.Fatalf("PostgreSQL %d: every comparison had the catalog:\n%s", major, out)
			}
			if code, out := run("db", "push", "--schema", after, "--rename", "app.t.net>app.t.amount"); code != 0 {
				t.Fatalf("PostgreSQL %d: push failed (%d):\n%s", major, code, out)
			}
			if code, out := run("db", "push", "--dry-run", "--schema", after); code != 0 || !strings.Contains(out, "in sync") {
				t.Fatalf("PostgreSQL %d: must converge (%d):\n%s", major, code, out)
			}
			if c.gated {
				if got := q09Query(t, fx, `SELECT gross::text FROM app.t`); got != "15" {
					t.Fatalf("gross must be recomputed with the new expression, got %s", got)
				}
			}
		})
	}
}

// migrate generate --mode live writes a rename-only migration whose down
// file reverts it, and a rename plus a real change whose down file
// restores the old expression under the new name before renaming back.
func TestQ10GenerateLiveRename(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "q10genlive")
	bin := buildCLIBinary(t)
	work := t.TempDir()
	all := q10Elements{generated: "%s * 2", check: "%s > 0", exprIndex: "abs(%s)", partial: "%s > 1"}
	chg := all
	chg.generated = "%s * 3"
	before, after, changed := filepath.Join(work, "before.json"), filepath.Join(work, "after.json"), filepath.Join(work, "changed.json")
	writeFile(t, before, q10Doc("net", "net", all))
	writeFile(t, after, q10Doc("amount", "amount", all))
	writeFile(t, changed, q10Doc("amount", "amount", chg))
	major := q09ServerMajor(t, fx)
	mig := filepath.Join(work, "migrations")
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	if code, out := run("migrate", "generate", "--mode", "live", "--schema", before, "--dir", mig, "--name", "init"); code != 0 {
		t.Fatalf("generate init failed (%d):\n%s", code, out)
	}
	if code, out := run("migrate", "--dir", mig); code != 0 {
		t.Fatalf("migrate init failed (%d):\n%s", code, out)
	}
	if err := fx.Exec(context.Background(), `INSERT INTO app.t (id, net, label) VALUES (1, 5, 'x')`); err != nil {
		t.Fatal(err)
	}
	identity := q09Query(t, fx, q10Identity)
	defsBefore := q09Query(t, fx, q10Definitions)

	if code, out := run("migrate", "generate", "--mode", "live", "--schema", after, "--dir", mig, "--name", "rename", "--rename", "app.t.net>app.t.amount"); code != 0 {
		t.Fatalf("PostgreSQL %d: generate rename failed (%d):\n%s", major, code, out)
	}
	up := q10FileStatements(t, filepath.Join(mig, "002_rename.up.sql"))
	down := q10FileStatements(t, filepath.Join(mig, "002_rename.down.sql"))
	if len(up) != 1 || up[0] != `alter table "app"."t" rename column "net" to "amount"` {
		t.Fatalf("PostgreSQL %d: the up file must be the rename alone: %q", major, up)
	}
	if len(down) != 1 || down[0] != `alter table "app"."t" rename column "amount" to "net"` {
		t.Fatalf("PostgreSQL %d: the down file must be the rename back alone: %q", major, down)
	}
	if code, out := run("migrate", "--dir", mig); code != 0 {
		t.Fatalf("PostgreSQL %d: migrate rename failed (%d):\n%s", major, code, out)
	}
	if got := q09Query(t, fx, q10Identity); got != identity {
		t.Fatalf("PostgreSQL %d: no rewrite, re-add or rebuild expected: %s -> %s", major, identity, got)
	}
	if code, out := run("migrate", "generate", "--mode", "live", "--schema", after, "--dir", mig, "--name", "again"); code != 0 || !strings.Contains(out, "No applicable changes") {
		t.Fatalf("PostgreSQL %d: the renamed schema must converge (%d):\n%s", major, code, out)
	}
	if code, out := run("migrate", "down", "--dir", mig); code != 0 {
		t.Fatalf("PostgreSQL %d: migrate down failed (%d):\n%s", major, code, out)
	}
	if got := q09Query(t, fx, q10Definitions); got != defsBefore {
		t.Fatalf("PostgreSQL %d: down must restore the definitions:\n got %s\nwant %s", major, got, defsBefore)
	}
	if err := os.Remove(filepath.Join(mig, "002_rename.up.sql")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(mig, "002_rename.down.sql")); err != nil {
		t.Fatal(err)
	}

	code, out := run("migrate", "generate", "--mode", "live", "--schema", changed, "--dir", mig, "--name", "change", "--rename", "app.t.net>app.t.amount")
	if major < 17 {
		if code == 0 || !strings.Contains(out, `generated column "gross" changes its expression`) || strings.Contains(out, "could not be verified") {
			t.Fatalf("PostgreSQL %d: a real change must be refused as a change (%d):\n%s", major, code, out)
		}
		if _, err := os.Stat(filepath.Join(mig, "002_change.up.sql")); err == nil {
			t.Fatal("a refused generate must write nothing")
		}
		return
	}
	if code != 0 {
		t.Fatalf("generate change failed (%d):\n%s", code, out)
	}
	up = q10FileStatements(t, filepath.Join(mig, "002_change.up.sql"))
	down = q10FileStatements(t, filepath.Join(mig, "002_change.down.sql"))
	wantUp := []string{
		`alter table "app"."t" rename column "net" to "amount"`,
		`alter table "app"."t" alter column "gross" set expression as ((amount * (3)::numeric))`,
	}
	wantDown := []string{
		`alter table "app"."t" alter column "gross" set expression as ((amount * (2)::numeric))`,
		`alter table "app"."t" rename column "amount" to "net"`,
	}
	if strings.Join(up, "\n") != strings.Join(wantUp, "\n") || strings.Join(down, "\n") != strings.Join(wantDown, "\n") {
		t.Fatalf("rename plus change:\nup   %q\ndown %q", up, down)
	}
	if code, out := run("migrate", "--dir", mig); code != 0 {
		t.Fatalf("migrate change failed (%d):\n%s", code, out)
	}
	if got := q09Query(t, fx, `SELECT gross::text FROM app.t`); got != "15" {
		t.Fatalf("gross must be recomputed, got %s", got)
	}
	if code, out := run("migrate", "down", "--dir", mig); code != 0 {
		t.Fatalf("migrate down of the change failed (%d):\n%s", code, out)
	}
	if got := q09Query(t, fx, q10Definitions); got != defsBefore {
		t.Fatalf("down must restore the definitions:\n got %s\nwant %s", got, defsBefore)
	}
	if got := q09Query(t, fx, `SELECT gross::text FROM app.t`); got != "10" {
		t.Fatalf("down must recompute gross with the old expression, got %s", got)
	}
}

// migrate generate --mode snapshot has no catalog: it cannot compare under
// the rename, so it must not claim the expressions are unchanged, and a
// plan of them as changes would carry down statements that name the old
// column before the rename is reverted. It refuses, names every element
// whose planning-base text predates the rename, and names the offline fix
// (Q11; TestQ11GenerateSnapshotRenameRefused follows it).
func TestQ10GenerateSnapshotRenameIsRefused(t *testing.T) {
	bin := buildCLIBinary(t)
	work := t.TempDir()
	all := q10Elements{generated: "%s * 2", check: "%s > 0", exprIndex: "abs(%s)", partial: "%s > 1"}
	before, after := filepath.Join(work, "before.json"), filepath.Join(work, "after.json")
	writeFile(t, before, q10Doc("net", "net", all))
	writeFile(t, after, q10Doc("amount", "amount", all))
	mig := filepath.Join(work, "migrations")
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, "postgres://offline.invalid/none", args...)
	}
	if code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", before, "--dir", mig, "--name", "init"); code != 0 {
		t.Fatalf("snapshot generate init failed (%d):\n%s", code, out)
	}
	code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", after, "--dir", mig, "--name", "rename", "--rename", "app.t.net>app.t.amount")
	if code == 0 {
		t.Fatalf("snapshot generate must refuse a rename its expressions depend on:\n%s", out)
	}
	for _, elem := range []string{
		`column gross generation expression: "amount * 2" (desired) vs "net * 2" (planning-base)`,
		`check constraint t_c expression: "amount > 0" (desired) vs "net > 0" (planning-base)`,
		`index app.t_expr_idx key part: "abs(amount)" (desired) vs "abs(net)" (planning-base)`,
		`index app.t_part_idx predicate: "amount > 1" (desired) vs "net > 1" (planning-base)`,
	} {
		if !strings.Contains(out, elem) {
			t.Fatalf("the refusal must name %q:\n%s", elem, out)
		}
	}
	if !strings.Contains(out, "predates the rename of net to amount") || !strings.Contains(out, "Plan it as two migrations instead") || strings.Contains(out, "live normalizer") {
		t.Fatalf("the refusal must name the rename and the offline fix:\n%s", out)
	}
	if m, _ := filepath.Glob(filepath.Join(mig, "002_*")); len(m) != 0 {
		t.Fatalf("a refused plan writes nothing: %v", m)
	}
}

// q10IncludeDoc renders app.t with column col and index t_inc_idx on (id)
// include (col) where pred.
func q10IncludeDoc(col, pred string) string {
	return fmt.Sprintf(`{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "app"}], "enums": [],
	"tables": [{"identity": {"schema": "app", "name": "t"}, "managed": true,
		"columns": [
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			{"name": %[1]q, "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false}],
		"constraints": [{"name": "t_pkey", "type": "primary-key", "columns": ["id"]}],
		"indexes": [{"identity": {"schema": "app", "name": "t_inc_idx"}, "unique": false, "method": "btree", "key": [{"column": "id"}], "include": [%[1]q], "where": %[2]q}]}],
	"views": [], "opaque": []
}`, col, pred)
}

// A renamed column in an index's INCLUDE list: the down file recreates the
// index under the renamed spelling in full (it runs before the rename is
// reverted), so migrate down restores the original definition.
func TestQ10GenerateLiveRenameInclude(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "q10geninc")
	bin := buildCLIBinary(t)
	work := t.TempDir()
	before, after, changed := filepath.Join(work, "before.json"), filepath.Join(work, "after.json"), filepath.Join(work, "changed.json")
	writeFile(t, before, q10IncludeDoc("net", "net > 1"))
	writeFile(t, after, q10IncludeDoc("amount", "amount > 1"))
	writeFile(t, changed, q10IncludeDoc("amount", "amount > 2"))
	major := q09ServerMajor(t, fx)
	mig := filepath.Join(work, "migrations")
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	if code, out := run("migrate", "generate", "--mode", "live", "--schema", before, "--dir", mig, "--name", "init"); code != 0 {
		t.Fatalf("generate init failed (%d):\n%s", code, out)
	}
	if code, out := run("migrate", "--dir", mig); code != 0 {
		t.Fatalf("migrate init failed (%d):\n%s", code, out)
	}
	if err := fx.Exec(context.Background(), `INSERT INTO app.t (id, net) VALUES (1, 5)`); err != nil {
		t.Fatal(err)
	}
	identity := q09Query(t, fx, q10Identity)
	defsBefore := q09Query(t, fx, q10Definitions)
	if want := `CREATE INDEX t_inc_idx ON app.t USING btree (id) INCLUDE (net) WHERE (net > (1)::numeric)`; defsBefore != want {
		t.Fatalf("PostgreSQL %d: initial definition %s", major, defsBefore)
	}

	// A rename alone: the INCLUDE list follows the rename in the catalog.
	if code, out := run("migrate", "generate", "--mode", "live", "--schema", after, "--dir", mig, "--name", "rename", "--rename", "app.t.net>app.t.amount"); code != 0 {
		t.Fatalf("PostgreSQL %d: generate rename failed (%d):\n%s", major, code, out)
	}
	up := q10FileStatements(t, filepath.Join(mig, "002_rename.up.sql"))
	down := q10FileStatements(t, filepath.Join(mig, "002_rename.down.sql"))
	if len(up) != 1 || up[0] != `alter table "app"."t" rename column "net" to "amount"` ||
		len(down) != 1 || down[0] != `alter table "app"."t" rename column "amount" to "net"` {
		t.Fatalf("PostgreSQL %d: a rename alone is the rename and its reverse:\nup   %q\ndown %q", major, up, down)
	}
	if code, out := run("migrate", "--dir", mig); code != 0 {
		t.Fatalf("PostgreSQL %d: migrate rename failed (%d):\n%s", major, code, out)
	}
	if got := q09Query(t, fx, q10Identity); got != identity {
		t.Fatalf("PostgreSQL %d: no rebuild expected: %s -> %s", major, identity, got)
	}
	if code, out := run("migrate", "down", "--dir", mig); code != 0 {
		t.Fatalf("PostgreSQL %d: migrate down failed (%d):\n%s", major, code, out)
	}
	if got := q09Query(t, fx, q10Definitions); got != defsBefore {
		t.Fatalf("PostgreSQL %d: down must restore the definition:\n got %s\nwant %s", major, got, defsBefore)
	}
	for _, f := range []string{"002_rename.up.sql", "002_rename.down.sql"} {
		if err := os.Remove(filepath.Join(mig, f)); err != nil {
			t.Fatal(err)
		}
	}

	// A rename plus a changed predicate rebuilds the index; its down
	// recreates the old predicate with the INCLUDE list renamed as well.
	if code, out := run("migrate", "generate", "--mode", "live", "--schema", changed, "--dir", mig, "--name", "change", "--rename", "app.t.net>app.t.amount"); code != 0 {
		t.Fatalf("PostgreSQL %d: generate change failed (%d):\n%s", major, code, out)
	}
	up = q10FileStatements(t, filepath.Join(mig, "002_change.up.sql"))
	down = q10FileStatements(t, filepath.Join(mig, "002_change.down.sql"))
	wantUp := []string{
		`alter table "app"."t" rename column "net" to "amount"`,
		`drop index if exists "app"."t_inc_idx"`,
		`create index "t_inc_idx" on "app"."t" using btree ("id") include ("amount") where (amount > (2)::numeric)`,
	}
	wantDown := []string{
		`drop index if exists "app"."t_inc_idx"`,
		`create index "t_inc_idx" on "app"."t" using btree ("id") include ("amount") where (amount > (1)::numeric)`,
		`alter table "app"."t" rename column "amount" to "net"`,
	}
	if strings.Join(up, "\n") != strings.Join(wantUp, "\n") || strings.Join(down, "\n") != strings.Join(wantDown, "\n") {
		t.Fatalf("PostgreSQL %d: rename plus predicate change:\nup   %q\ndown %q", major, up, down)
	}
	if code, out := run("migrate", "--dir", mig, "--allow-destructive"); code != 0 {
		t.Fatalf("PostgreSQL %d: migrate change failed (%d):\n%s", major, code, out)
	}
	if got, want := q09Query(t, fx, q10Definitions), `CREATE INDEX t_inc_idx ON app.t USING btree (id) INCLUDE (amount) WHERE (amount > (2)::numeric)`; got != want {
		t.Fatalf("PostgreSQL %d: definition after the change:\n got %s\nwant %s", major, got, want)
	}
	if code, out := run("migrate", "down", "--dir", mig); code != 0 {
		t.Fatalf("PostgreSQL %d: migrate down of the change failed (%d):\n%s", major, code, out)
	}
	if got := q09Query(t, fx, q10Definitions); got != defsBefore {
		t.Fatalf("PostgreSQL %d: down must restore the definition:\n got %s\nwant %s", major, got, defsBefore)
	}
	if got := q09Query(t, fx, `SELECT row_to_json(t)::text FROM app.t t`); got != `{"id":1,"net":5}` {
		t.Fatalf("PostgreSQL %d: rows changed: %s", major, got)
	}
}

// When the live table cannot be copied to compare it under the rename (a
// whole-row reference in a check names the table, and the copy has another
// name), a live run refuses on every server version (Q11: the plan's down
// would carry the old name) and names the fix that works: rename the
// column by hand, which lets PostgreSQL rewrite the expressions, and
// re-run without --rename. It never says to spell the expression as the
// database does (the database spells it with the old name) or to re-run
// with a live normalizer (this run had one).
func TestQ10UnrenamedLiveRunAdvice(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "q10advice")
	bin := buildCLIBinary(t)
	work := t.TempDir()
	after := filepath.Join(work, "after.json")
	writeFile(t, after, `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "app"}], "enums": [],
	"tables": [{"identity": {"schema": "app", "name": "t"}, "managed": true,
		"columns": [
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			{"name": "amount", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false},
			{"name": "gross", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false, "generated": {"expression": "(amount * (2)::numeric)"}}],
		"constraints": [{"name": "t_pkey", "type": "primary-key", "columns": ["id"]},
			{"name": "t_row", "type": "check", "expression": "(row_to_json(t.*) IS NOT NULL)"}],
		"indexes": []}],
	"views": [], "opaque": []
}`)
	major := q09ServerMajor(t, fx)
	for _, stmt := range []string{
		`CREATE SCHEMA app`,
		`CREATE TABLE app.t (id int4 PRIMARY KEY, net numeric, gross numeric GENERATED ALWAYS AS (net * 2) STORED, CONSTRAINT t_row CHECK (row_to_json(t.*) IS NOT NULL))`,
		`INSERT INTO app.t (id, net) VALUES (1, 5)`,
	} {
		if err := fx.Exec(context.Background(), stmt); err != nil {
			t.Fatal(err)
		}
	}
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	const byHand = `Rename the column by hand first: alter table "app"."t" rename column "net" to "amount" (PostgreSQL rewrites the expressions that reference it), then re-run without the --rename flags for app.t (keep any others)`
	code, out := run("db", "push", "--dry-run", "--schema", after, "--rename", "app.t.net>app.t.amount")
	if code == 0 || !strings.Contains(out, "The plan is refused") {
		t.Fatalf("PostgreSQL %d: a comparison that could not follow the rename is refused (%d):\n%s", major, code, out)
	}
	if !strings.Contains(out, `column gross generation expression: "(amount * (2)::numeric)" (desired) vs "(net * (2)::numeric)" (live)`) || !strings.Contains(out, "predates the rename of net to amount") || !strings.Contains(out, byHand) {
		t.Fatalf("PostgreSQL %d: the refusal must name the element, the rename and the hand rename:\n%s", major, out)
	}
	for _, bad := range []string{"spells it", "live normalizer"} {
		if strings.Contains(out, bad) {
			t.Fatalf("PostgreSQL %d: %q is not advice a live rename run can follow:\n%s", major, bad, out)
		}
	}

	// Following the advice: after the hand rename, the plan without
	// --rename compares the renamed text and finds nothing to do.
	if err := fx.Exec(context.Background(), `ALTER TABLE app.t RENAME COLUMN net TO amount`); err != nil {
		t.Fatal(err)
	}
	if code, out := run("db", "push", "--dry-run", "--schema", after); code != 0 || !strings.Contains(out, "in sync") {
		t.Fatalf("PostgreSQL %d: after the hand rename the schema is in sync (%d):\n%s", major, code, out)
	}
}
