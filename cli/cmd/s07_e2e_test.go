package cmd

// S07 regressions through the REAL CLI binary against disposable
// databases. Skipped unless NEUTRON_E2E_DATABASE_URL is set
// (NEUTRON_LIVE_REQUIRED=1 fails instead).

import (
	"context"
	"encoding/json"
	"os"
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

// TestS07ChainIncompleteHintAfterBaseline: a hand-authored file in a
// baselined snapshot chain is refused, and the hint names steps that work
// while the baseline exists (`schema baseline` alone refuses then); the
// re-baseline route it gives is followed to a clean check.
func TestS07ChainIncompleteHintAfterBaseline(t *testing.T) {
	bin := buildCLIBinary(t)
	dbURL, fx := newM02CommandDB(t, "s07chain")
	if err := fx.Exec(context.Background(), `CREATE TABLE t (id int PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	mig := filepath.Join(t.TempDir(), "migrations")
	must := func(args ...string) string {
		t.Helper()
		code, out := runCLIProcess(t, bin, dbURL, args...)
		if code != 0 {
			t.Fatalf("neutron %s exited %d:\n%s", strings.Join(args, " "), code, out)
		}
		return out
	}
	must("schema", "baseline", "--dir", mig)
	writeFile(t, filepath.Join(mig, "001_hand.up.sql"), "ALTER TABLE t ADD COLUMN note text;\n")
	writeFile(t, filepath.Join(mig, "001_hand.down.sql"), "ALTER TABLE t DROP COLUMN note;\n")

	code, out := runCLIProcess(t, bin, dbURL, "migrate", "--dir", mig)
	if code == 0 {
		t.Fatalf("a file without a snapshot must be refused:\n%s", out)
	}
	for _, w := range []string{
		"001_hand has a .up.sql file but no snapshot — the snapshot chain is incomplete",
		"move the file out of the migrations directory and plan its change with `neutron migrate generate --mode snapshot`",
		"or re-baseline around it: delete migrations/snapshots, apply it with `neutron migrate`, then run `neutron schema baseline`",
	} {
		if !strings.Contains(out, w) {
			t.Fatalf("the refusal must say %q:\n%s", w, out)
		}
	}
	// The step the old hint named refuses while the baseline exists.
	if code, out := runCLIProcess(t, bin, dbURL, "schema", "baseline", "--dir", mig); code == 0 || !strings.Contains(out, "baseline snapshot already exists") {
		t.Fatalf("baseline must refuse while one exists (%d):\n%s", code, out)
	}

	// The hint's re-baseline route works as written.
	if err := os.RemoveAll(filepath.Join(mig, "snapshots")); err != nil {
		t.Fatal(err)
	}
	must("migrate", "--dir", mig)
	if out := must("schema", "baseline", "--dir", mig); !strings.Contains(out, "covered by the baseline: 001") {
		t.Fatalf("the re-baseline covers the applied file:\n%s", out)
	}
	must("schema", "check", "--live", "--dir", mig)
	if got := q09Query(t, fx, `SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 't'`); got != "id,note" {
		t.Fatalf("t columns %s", got)
	}
}

// TestS07NontransactionalFailureWording: a nontransactional migration that
// fails at its first statement says so (nothing of it ran before), and one
// that fails later still reports the earlier statements' effects as
// remaining (Q09 review-2 INFO 4).
func TestS07NontransactionalFailureWording(t *testing.T) {
	bin := buildCLIBinary(t)
	for _, c := range []struct {
		name, up string
		want     []string
		notWant  []string
	}{
		{name: "first statement",
			up:      "-- Migration: idx\nCREATE INDEX CONCURRENTLY t_nope ON t (nope);\n",
			want:    []string{"002_idx failed at its first statement, outside any transaction; no earlier statement of it ran", "statement 1 of 1 failed; no earlier statement had run", "neutron migrate resolve 002"},
			notWant: []string{"MID-FILE", "REMAIN", "already taken effect"}},
		{name: "second statement",
			up:      "-- Migration: idx\nCREATE INDEX CONCURRENTLY t_v ON t (v);\nCREATE INDEX CONCURRENTLY t_nope ON t (nope);\n",
			want:    []string{"002_idx failed MID-FILE outside any transaction; its earlier statements' effects REMAIN", "statement 2 of 2 failed after 1 statement(s) had already taken effect", "neutron migrate resolve 002"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			dbURL, _ := newM02CommandDB(t, "s07ntx")
			mig := filepath.Join(t.TempDir(), "migrations")
			if err := os.MkdirAll(mig, 0o755); err != nil {
				t.Fatal(err)
			}
			writeFile(t, filepath.Join(mig, "001_init.up.sql"), "CREATE TABLE t (id int PRIMARY KEY, v int);\n")
			writeFile(t, filepath.Join(mig, "001_init.down.sql"), "DROP TABLE t;\n")
			writeFile(t, filepath.Join(mig, "002_idx.up.sql"), c.up)
			writeFile(t, filepath.Join(mig, "002_idx.down.sql"), "DROP INDEX IF EXISTS t_v;\n")
			code, out := runCLIProcess(t, bin, dbURL, "migrate", "--dir", mig)
			if code == 0 {
				t.Fatalf("the migration must fail:\n%s", out)
			}
			for _, w := range c.want {
				if !strings.Contains(out, w) {
					t.Errorf("output must say %q:\n%s", w, out)
				}
			}
			for _, w := range c.notWant {
				if strings.Contains(out, w) {
					t.Errorf("output must not say %q:\n%s", w, out)
				}
			}
		})
	}
}
