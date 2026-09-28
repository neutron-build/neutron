package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestS07R1DollarIdentifierCannotHideAStatement: S07 review-1 F2 end to
// end. A view definition "select 1 as a$x$; drop table ...; select 1 as
// b$x$" read "$x$...$x$" as a dollar quote, so the validators, the
// destructive classification and the guards saw one statement while
// PostgreSQL ran three and dropped an undeclared table without
// --allow-destructive. Layer a: every scanner continues an identifier
// through '$'. Layer b: every applied statement runs on its own over the
// extended protocol, which refuses more than one command; a statement that
// turns standard_conforming_strings off, which would make the server read
// the next statement differently than the checks did, is refused.
func TestS07R1DollarIdentifierCannotHideAStatement(t *testing.T) {
	bin := buildCLIBinary(t)
	const def = "select 1 as a$x$; drop table if exists public.victim; select 1 as b$x$"
	setup := func(t *testing.T, label string) (string, func() string, string) {
		t.Helper()
		dbURL, fx := newM02CommandDB(t, label)
		if err := fx.Exec(context.Background(), `CREATE TABLE victim (id int PRIMARY KEY); INSERT INTO victim VALUES (1)`); err != nil {
			t.Fatal(err)
		}
		victim := func() string {
			return q09Query(t, fx, `SELECT count(*)::text FROM pg_class WHERE relname = 'victim' AND relkind = 'r'`)
		}
		return dbURL, victim, t.TempDir()
	}
	refused := func(t *testing.T, bin, dbURL string, want []string, args ...string) {
		t.Helper()
		code, out := runCLIProcess(t, bin, dbURL, args...)
		if code == 0 {
			t.Fatalf("neutron %s must be refused:\n%s", strings.Join(args, " "), out)
		}
		for _, w := range want {
			if !strings.Contains(out, w) {
				t.Fatalf("neutron %s output must mention %q:\n%s", strings.Join(args, " "), w, out)
			}
		}
	}
	doc := `{"version": 2, "dialect": "postgresql", "capabilities": [], "schemas": [{"name": "public"}], "tables": [], "enums": [],
		"views": [{"identity": {"schema": "public", "name": "v"}, "managed": true, "definition": "` + def + `"}], "opaque": []}`

	t.Run("db push and generate refuse the document", func(t *testing.T) {
		dbURL, victim, work := setup(t, "s07r1push")
		path := filepath.Join(work, "d.json")
		writeFile(t, path, doc)
		want := []string{"view definition must be a single statement"}
		refused(t, bin, dbURL, want, "db", "push", "--schema", path)
		refused(t, bin, dbURL, want, "migrate", "generate", "--schema", path, "--dir", filepath.Join(work, "migrations"), "--name", "v")
		if victim() != "1" {
			t.Fatal("victim was dropped")
		}
	})

	t.Run("migrate classifies the hidden drop", func(t *testing.T) {
		dbURL, victim, work := setup(t, "s07r1mig")
		mig := filepath.Join(work, "migrations")
		if err := os.MkdirAll(mig, 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(mig, "001_v.up.sql"), "create view v as "+def+";\n")
		writeFile(t, filepath.Join(mig, "001_v.down.sql"), "drop view v;\n")
		refused(t, bin, dbURL, []string{"drop table if exists public.victim", "--allow-destructive"}, "migrate", "--dir", mig)
		if victim() != "1" {
			t.Fatal("victim was dropped without --allow-destructive")
		}
	})

	t.Run("migrate keeps the literal rules the checks read with", func(t *testing.T) {
		dbURL, victim, work := setup(t, "s07r1ext")
		mig := filepath.Join(work, "migrations")
		if err := os.MkdirAll(mig, 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(mig, "001_scs.up.sql"), "SET LOCAL standard_conforming_strings = off;\nselect 'a\\''; drop table victim; select 'b';\n")
		writeFile(t, filepath.Join(mig, "001_scs.down.sql"), "select 1;\n")
		refused(t, bin, dbURL, []string{"turned standard_conforming_strings off", "refused"}, "migrate", "--dir", mig)
		if victim() != "1" {
			t.Fatal("victim was dropped by a statement no check saw")
		}
	})
}
