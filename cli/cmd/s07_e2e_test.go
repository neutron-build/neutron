package cmd

// S07 regressions through the REAL CLI binary against disposable
// databases. Skipped unless NEUTRON_E2E_DATABASE_URL is set
// (NEUTRON_LIVE_REQUIRED=1 fails instead).

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestS07PushDottedRenameTarget: the --rename flag Studio gives for a new
// column name containing a dot parses (the column is everything after the
// table), and the push plans only the rename: the key constraint and the
// index on the column are not rebuilt.
func TestS07PushDottedRenameTarget(t *testing.T) {
	bin := buildCLIBinary(t)
	dbURL, fx := newM02CommandDB(t, "s07dot")
	if err := fx.Exec(context.Background(), `CREATE SCHEMA app; CREATE TABLE app.t (id int PRIMARY KEY, net numeric NOT NULL, CONSTRAINT t_u UNIQUE (net)); CREATE INDEX t_n ON app.t (net); INSERT INTO app.t VALUES (1, 5)`); err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	pulled, after := filepath.Join(work, "pulled.json"), filepath.Join(work, "after.json")
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}
	if code, out := run("schema", "pull", "--out", pulled); code != 0 {
		t.Fatalf("pull (%d):\n%s", code, out)
	}
	doc, err := db.ParseV2Document(mustReadFile(t, pulled))
	if err != nil {
		t.Fatal(err)
	}
	m, err := db.ModelFromRoot(doc.Root)
	if err != nil {
		t.Fatal(err)
	}
	renamed := func(c string) string {
		if c == "net" {
			return "a.b"
		}
		return c
	}
	for ti := range m.Tables {
		tb := &m.Tables[ti]
		if tb.Identity.Schema != "app" || tb.Identity.Name != "t" {
			continue
		}
		for ci := range tb.Columns {
			tb.Columns[ci].Name = renamed(tb.Columns[ci].Name)
		}
		for ci := range tb.Constraints {
			for i, c := range tb.Constraints[ci].Columns {
				tb.Constraints[ci].Columns[i] = renamed(c)
			}
		}
		for ii := range tb.Indexes {
			for ki := range tb.Indexes[ii].Key {
				if c := tb.Indexes[ii].Key[ki].Column; c != nil {
					name := renamed(*c)
					tb.Indexes[ii].Key[ki].Column = &name
				}
			}
		}
	}
	root, err := db.RootFromModel(m)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, after, string(raw))

	const flag = "app.t.net>app.t.a.b"
	rename := `alter table "app"."t" rename column "net" to "a.b"`
	code, out := run("db", "push", "--dry-run", "--schema", after, "--rename", flag)
	if code != 0 {
		t.Fatalf("the dotted rename flag must plan (%d):\n%s", code, out)
	}
	if stmts := q10Statements(out); len(stmts) != 1 || stmts[0] != rename {
		t.Fatalf("a rename alone must plan exactly %q, got %q:\n%s", rename, stmts, out)
	}
	if code, out := run("db", "push", "--schema", after, "--rename", flag); code != 0 {
		t.Fatalf("push (%d):\n%s", code, out)
	}
	if code, out := run("db", "push", "--dry-run", "--schema", after); code != 0 || !strings.Contains(out, "in sync") {
		t.Fatalf("the renamed schema must converge (%d):\n%s", code, out)
	}
	if got := q09Query(t, fx, `SELECT "a.b"::text FROM app.t`); got != "5" {
		t.Fatalf("row after rename: %s", got)
	}
}
