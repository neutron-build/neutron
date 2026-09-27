package cmd

// M08 live coverage through the REAL CLI binary: a snapshot plan without
// --allow-destructive leaves objects the schema no longer declares in
// place, and its target snapshot records them, so the next migrate and
// schema check --live keep working and a later --allow-destructive plan
// drops them. Chains written by earlier CLIs (target snapshot = the desired
// document) keep working after an upgrade. Skipped unless
// NEUTRON_E2E_DATABASE_URL is set (NEUTRON_LIVE_REQUIRED=1 fails instead).

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// m08FixtureSQL is an existing database: the application's table t with a
// column and an index it no longer declares, and a second application's
// table, view and enum.
const m08FixtureSQL = `CREATE TYPE mood AS ENUM ('ok', 'bad');
CREATE TABLE t (id serial PRIMARY KEY, keep text, old integer);
CREATE INDEX t_old_idx ON t (old);
CREATE TABLE other_app (id integer PRIMARY KEY, v text, m mood);
CREATE VIEW v_other AS SELECT id, v FROM other_app;
INSERT INTO t (keep, old) VALUES ('k1', 1), ('k2', 2);
INSERT INTO other_app VALUES (1, 'row', 'ok')`

const m08Columns = `SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 't'`

const m08Relations = `SELECT string_agg(relname, ',' ORDER BY relname) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE nspname = 'public' AND relkind IN ('r', 'v', 'i') AND relname NOT LIKE '\_neutron%'`

// m08Desired writes the pulled document minus what the application no
// longer declares (t.old, t_old_idx, other_app, v_other, mood), plus the
// integer columns in add.
func m08Desired(t *testing.T, pulled, out string, add ...string) {
	t.Helper()
	doc, err := db.ParseV2Document(mustReadFile(t, pulled))
	if err != nil {
		t.Fatal(err)
	}
	m, err := db.ModelFromRoot(doc.Root)
	if err != nil {
		t.Fatal(err)
	}
	var tables []db.V2Table
	for _, tb := range m.Tables {
		if tb.Identity.Name != "t" {
			continue
		}
		var cols []db.V2Column
		for _, c := range tb.Columns {
			if c.Name != "old" {
				cols = append(cols, c)
			}
		}
		for _, name := range add {
			cols = append(cols, db.V2Column{Name: name, Type: db.V2ColumnType{Name: "int4", Codec: "number"}})
		}
		tb.Columns, tb.Indexes = cols, []db.V2Index{}
		tables = append(tables, tb)
	}
	m.Tables, m.Views, m.Enums = tables, []db.V2View{}, []db.V2EnumDecl{}
	root, err := db.RootFromModel(m)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, out, string(raw))
}

// m08ColumnsAs writes the pulled document with table u's columns in the
// given order: existing columns by name, any other name as a new integer
// column.
func m08ColumnsAs(t *testing.T, pulled, out string, order ...string) {
	t.Helper()
	doc, err := db.ParseV2Document(mustReadFile(t, pulled))
	if err != nil {
		t.Fatal(err)
	}
	m, err := db.ModelFromRoot(doc.Root)
	if err != nil {
		t.Fatal(err)
	}
	for i := range m.Tables {
		if m.Tables[i].Identity.Name != "u" {
			continue
		}
		var cols []db.V2Column
		for _, name := range order {
			if c := m.Tables[i].Column(name); c != nil {
				cols = append(cols, *c)
			} else {
				cols = append(cols, db.V2Column{Name: name, Type: db.V2ColumnType{Name: "int4", Codec: "number"}})
			}
		}
		m.Tables[i].Columns = cols
	}
	root, err := db.RootFromModel(m)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, out, string(raw))
}

const m08UColumns = `SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'u'`

// m08PreM08Migration writes a snapshot migration exactly as CLIs before M08
// did: planned from the chain head, target snapshot = the desired document.
func m08PreM08Migration(t *testing.T, mig, version, name, desiredPath string) {
	t.Helper()
	chain, err := db.LoadSnapshotChain(mig)
	if err != nil {
		t.Fatal(err)
	}
	base, err := chain.HeadDocument()
	if err != nil {
		t.Fatal(err)
	}
	desired, err := db.ParseV2Document(mustReadFile(t, desiredPath))
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.DiffV2Document(context.Background(), desired, base, db.DiffV2Options{SnapshotBase: true})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := db.BuildPlanArtifact(version, name, chain.HeadRef, chain.HeadSHA256, desired, nil, res)
	if err != nil {
		t.Fatal(err)
	}
	files, err := db.MigrationArtifactSet(mig, version, name, plan, desired, strings.Join(res.Up, ";\n")+";", strings.Join(reverseStrings(res.Down), ";\n")+";")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WriteArtifactSet(files); err != nil {
		t.Fatal(err)
	}
}

func TestM08LeftInPlaceSnapshotChain(t *testing.T) {
	if os.Getenv("NEUTRON_E2E_DATABASE_URL") == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; M08 E2E skipped (set it to a disposable Postgres URL to run)")
	}
	bin := buildCLIBinary(t)
	unreachable := "postgres://m08-no-such-user@127.0.0.1:1/m08offline"

	must := func(t *testing.T, url string, args ...string) string {
		t.Helper()
		code, out := runCLIProcess(t, bin, url, args...)
		if code != 0 {
			t.Fatalf("neutron %s exited %d:\n%s", strings.Join(args, " "), code, out)
		}
		return out
	}
	refused := func(t *testing.T, url string, want []string, args ...string) {
		t.Helper()
		code, out := runCLIProcess(t, bin, url, args...)
		if code == 0 {
			t.Fatalf("neutron %s must fail:\n%s", strings.Join(args, " "), out)
		}
		for _, w := range want {
			if !strings.Contains(out, w) {
				t.Fatalf("neutron %s output must mention %q:\n%s", strings.Join(args, " "), w, out)
			}
		}
	}
	setup := func(t *testing.T, label string) (dbURL string, fx *db.Client, work, mig string) {
		t.Helper()
		dbURL, fx = newM02CommandDB(t, label)
		if err := fx.Exec(context.Background(), m08FixtureSQL); err != nil {
			t.Fatal(err)
		}
		work = t.TempDir()
		mig = filepath.Join(work, "migrations")
		must(t, dbURL, "schema", "baseline", "--dir", mig)
		must(t, dbURL, "schema", "pull", "--out", filepath.Join(work, "pulled.json"))
		m08Desired(t, filepath.Join(work, "pulled.json"), filepath.Join(work, "d1.json"), "note")
		m08Desired(t, filepath.Join(work, "pulled.json"), filepath.Join(work, "d2.json"), "note", "note2")
		return
	}
	// dropAndCheck is the explicit --allow-destructive drop of everything
	// left in place, applied and checked.
	dropAndCheck := func(t *testing.T, dbURL string, fx *db.Client, work, mig, version string) {
		t.Helper()
		must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", filepath.Join(work, "d2.json"), "--name", "drop_untouched", "--allow-destructive")
		up := string(mustReadFile(t, filepath.Join(mig, version+"_drop_untouched.up.sql")))
		for _, want := range []string{`drop view if exists "public"."v_other"`, `drop column if exists "old"`, `drop index if exists "public"."t_old_idx"`, `drop table if exists "public"."other_app"`, `drop type if exists "public"."mood"`} {
			if !strings.Contains(up, want) {
				t.Fatalf("the destructive plan must contain %s:\n%s", want, up)
			}
		}
		refused(t, dbURL, []string{"--allow-destructive"}, "migrate", "--dir", mig)
		must(t, dbURL, "migrate", "--dir", mig, "--allow-destructive")
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		must(t, dbURL, "schema", "check", "--dir", mig, "--schema", filepath.Join(work, "d2.json"))
		if out := must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", filepath.Join(work, "d2.json")); !strings.Contains(out, "No schema changes detected") {
			t.Fatalf("nothing is left to plan:\n%s", out)
		}
		if got := q09Query(t, fx, m08Columns); got != "id,keep,note,note2" {
			t.Fatalf("t columns %s", got)
		}
		if got := q09Query(t, fx, m08Relations); got != "t,t_pkey" {
			t.Fatalf("relations %s", got)
		}
		if got := q09Query(t, fx, `SELECT string_agg(keep, ',' ORDER BY id) FROM t`); got != "k1,k2" {
			t.Fatalf("t rows %s", got)
		}
	}

	// The card's exit: baseline -> a plan that omits a column and a table
	// (and an index, a view and an enum) -> migrate -> the next snapshot
	// migration -> check --live -> an explicit --allow-destructive drop.
	t.Run("LeftInPlaceThenDropped", func(t *testing.T) {
		dbURL, fx, work, mig := setup(t, "m08fresh")
		out := must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", filepath.Join(work, "d1.json"), "--name", "add_note")
		if up := string(mustReadFile(t, filepath.Join(mig, "001_add_note.up.sql"))); strings.Contains(up, "drop") || !strings.Contains(out, "left untouched") {
			t.Fatalf("a plan without --allow-destructive drops nothing:\n%s\n%s", out, up)
		}
		must(t, dbURL, "migrate", "--dir", mig)
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		for _, want := range []string{"the target snapshot records what this plan leaves in place", "table public.other_app", "column public.t.old", "index public.t_old_idx on table public.t", "view public.v_other", "enum public.mood"} {
			if !strings.Contains(out, want) {
				t.Fatalf("generate output must mention %q:\n%s", want, out)
			}
		}

		out = must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", filepath.Join(work, "d2.json"), "--name", "add_note2")
		if !strings.Contains(out, "left untouched") || strings.Contains(out, "will be dropped") {
			t.Fatalf("the next plan reports them as left in place again:\n%s", out)
		}
		must(t, dbURL, "migrate", "--dir", mig)
		if out := must(t, dbURL, "schema", "check", "--live", "--dir", mig); !strings.Contains(out, "002_add_note2") {
			t.Fatalf("live check compares against the applied snapshot:\n%s", out)
		}
		if got := q09Query(t, fx, m08Columns); got != "id,keep,old,note,note2" {
			t.Fatalf("t columns %s", got)
		}
		// Offline, the desired document lacks what the chain records.
		refused(t, unreachable, []string{"pending change", "other_app", "left in place", "declare them in the schema, or plan their drop with `neutron migrate generate --mode snapshot --allow-destructive`"}, "schema", "check", "--dir", mig, "--schema", filepath.Join(work, "d2.json"))
		dropAndCheck(t, dbURL, fx, work, mig, "003")
	})

	// An out-of-band change to an object left in place is drift.
	t.Run("RealDriftStillRefused", func(t *testing.T) {
		dbURL, fx, work, mig := setup(t, "m08drift")
		must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", filepath.Join(work, "d1.json"), "--name", "add_note")
		must(t, dbURL, "migrate", "--dir", mig)
		must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", filepath.Join(work, "d2.json"), "--name", "add_note2")
		ctx := context.Background()
		for _, c := range []struct{ change, revert, want string }{
			{`ALTER TABLE t ALTER COLUMN old TYPE bigint`, `ALTER TABLE t ALTER COLUMN old TYPE integer`, `alter column "old" type integer`},
			{`DROP VIEW v_other`, `CREATE VIEW v_other AS SELECT id, v FROM other_app`, `v_other`},
			{`ALTER TABLE other_app ADD COLUMN rogue integer`, `ALTER TABLE other_app DROP COLUMN rogue`, `rogue`},
		} {
			if err := fx.Exec(ctx, c.change); err != nil {
				t.Fatal(err)
			}
			refused(t, dbURL, []string{"drift", c.want}, "schema", "check", "--live", "--dir", mig)
			refused(t, dbURL, []string{"drift", c.want}, "migrate", "--dir", mig)
			if got := q09Query(t, fx, m08Columns); got != "id,keep,old,note" {
				t.Fatalf("a refused migrate applied something: t columns %s", got)
			}
			if err := fx.Exec(ctx, c.revert); err != nil {
				t.Fatal(err)
			}
		}
		must(t, dbURL, "migrate", "--dir", mig)
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
	})

	// With --allow-destructive, an enum whose last column the same plan
	// drops is not dropped; the target records it, and the next
	// destructive plan drops it.
	t.Run("EnumStillUsedByADroppedColumn", func(t *testing.T) {
		dbURL, fx := newM02CommandDB(t, "m08enum")
		if err := fx.Exec(context.Background(), `CREATE TYPE mood AS ENUM ('ok'); CREATE TABLE t (id integer PRIMARY KEY, status mood)`); err != nil {
			t.Fatal(err)
		}
		work := t.TempDir()
		mig := filepath.Join(work, "migrations")
		must(t, dbURL, "schema", "baseline", "--dir", mig)
		desired := filepath.Join(work, "d.json")
		writeFile(t, desired, `{"version": 2, "dialect": "postgresql", "capabilities": [], "schemas": [{"name": "public"}],
			"tables": [{"identity": {"schema": "public", "name": "t"}, "managed": true,
				"columns": [{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}],
				"constraints": [{"name": "t_pkey", "type": "primary-key", "columns": ["id"]}], "indexes": []}],
			"enums": [], "views": [], "opaque": []}`)
		out := must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", desired, "--name", "drop_status", "--allow-destructive")
		if !strings.Contains(out, "still used") || !strings.Contains(out, "enum public.mood") {
			t.Fatalf("the enum stays and is recorded:\n%s", out)
		}
		must(t, dbURL, "migrate", "--dir", mig, "--allow-destructive")
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", desired, "--name", "drop_mood", "--allow-destructive")
		must(t, dbURL, "migrate", "--dir", mig, "--allow-destructive")
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		if got := q09Query(t, fx, `SELECT count(*)::text FROM pg_type WHERE typname = 'mood'`); got != "0" {
			t.Fatalf("mood must be dropped by the second plan")
		}
	})

	// The recovery for a chain an earlier CLI left drifted (M08 review-2):
	// the chain is read as recorded, so the drift the pre-M08 snapshot
	// causes is refused with a hint naming the re-baseline, and the
	// re-baseline recovers it. 001 is applied, 002 is pending, both written
	// the pre-M08 way.
	t.Run("EarlierCLIChainRebaseline", func(t *testing.T) {
		dbURL, fx, work, mig := setup(t, "m08old")
		m08PreM08Migration(t, mig, "001", "add_note", filepath.Join(work, "d1.json"))
		must(t, dbURL, "migrate", "--dir", mig)
		m08PreM08Migration(t, mig, "002", "add_note2", filepath.Join(work, "d2.json"))
		snap001 := filepath.Join(mig, "snapshots", "001_add_note.snapshot.json")
		if got := strings.Join(m07DocTables(t, snap001), ","); got != "public.t" {
			t.Fatalf("fixture must be the pre-M08 snapshot shape, lists %s", got)
		}
		hint := []string{"drift", "only of objects the database has and the applied snapshot does not record", "delete migrations/snapshots", "neutron schema baseline"}
		refused(t, dbURL, hint, "schema", "check", "--live", "--dir", mig)
		refused(t, dbURL, hint, "migrate", "--dir", mig)
		if got := q09Query(t, fx, m08Columns); got != "id,keep,old,note" {
			t.Fatalf("a refused migrate applied something: t columns %s", got)
		}

		// The recovery the hint names: the pending 002 moves out (it is
		// generated again), the snapshots go, the baseline records the
		// database and covers the applied 001.
		for _, f := range []string{"002_add_note2.up.sql", "002_add_note2.down.sql", "002_add_note2.plan.json"} {
			if err := os.Remove(filepath.Join(mig, f)); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.RemoveAll(filepath.Join(mig, "snapshots")); err != nil {
			t.Fatal(err)
		}
		if out := must(t, dbURL, "schema", "baseline", "--dir", mig); !strings.Contains(out, "covered by the baseline: 001") {
			t.Fatalf("the re-baseline covers the applied file:\n%s", out)
		}
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		out := must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", filepath.Join(work, "d2.json"), "--name", "add_note2")
		if !strings.Contains(out, "the target snapshot records what this plan leaves in place") {
			t.Fatalf("the regenerated 002 records what stays:\n%s", out)
		}
		must(t, dbURL, "migrate", "--dir", mig)
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		if got := q09Query(t, fx, m08Columns); got != "id,keep,old,note,note2" {
			t.Fatalf("t columns %s", got)
		}
		dropAndCheck(t, dbURL, fx, work, mig, "003")
	})

	// P1: the schema declares a new column between existing ones.
	// PostgreSQL appends it; the snapshot records the database order, and
	// the next plan compares columns by name. writeFirst writes 001 the
	// pre-M08 way (declared order recorded) instead of through the CLI.
	midTable := func(t *testing.T, label string, writeFirst bool) {
		dbURL, fx := newM02CommandDB(t, label)
		if err := fx.Exec(context.Background(), `CREATE TABLE u (id serial PRIMARY KEY, a text, b text); INSERT INTO u (a, b) VALUES ('a1', 'b1')`); err != nil {
			t.Fatal(err)
		}
		work := t.TempDir()
		mig := filepath.Join(work, "migrations")
		pulled := filepath.Join(work, "pulled.json")
		d1, d2 := filepath.Join(work, "d1.json"), filepath.Join(work, "d2.json")
		must(t, dbURL, "schema", "baseline", "--dir", mig)
		must(t, dbURL, "schema", "pull", "--out", pulled)
		m08ColumnsAs(t, pulled, d1, "id", "a", "mid", "b")
		m08ColumnsAs(t, pulled, d2, "id", "mid2", "a", "mid", "b")

		out := ""
		if writeFirst {
			m08PreM08Migration(t, mig, "001", "add_mid", d1)
		} else {
			out = must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", d1, "--name", "add_mid")
		}
		must(t, dbURL, "migrate", "--dir", mig)
		if got := q09Query(t, fx, m08UColumns); got != "id,a,b,mid" {
			t.Fatalf("u columns %s", got)
		}
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		if !writeFirst && !strings.Contains(out, "the database holds them as (id, a, b, mid)") {
			t.Fatalf("generate must note the database order:\n%s", out)
		}
		must(t, dbURL, "schema", "check", "--dir", mig, "--schema", d1)

		must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", d2, "--name", "add_mid2")
		if up := string(mustReadFile(t, filepath.Join(mig, "002_add_mid2.up.sql"))); !strings.Contains(up, `add column "mid2"`) || strings.Count(up, ";") != 1 {
			t.Fatalf("the next plan adds mid2 only:\n%s", up)
		}
		must(t, dbURL, "migrate", "--dir", mig)
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		must(t, dbURL, "schema", "check", "--dir", mig, "--schema", d2)
		if out := must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", d2); !strings.Contains(out, "No schema changes detected") {
			t.Fatalf("an order difference alone plans nothing:\n%s", out)
		}
		if got := q09Query(t, fx, m08UColumns); got != "id,a,b,mid,mid2" {
			t.Fatalf("u columns %s", got)
		}
		if got := q09Query(t, fx, `SELECT a || b FROM u`); got != "a1b1" {
			t.Fatalf("u rows %s", got)
		}
	}
	t.Run("ColumnAddedMidTable", func(t *testing.T) { midTable(t, "m08mid", false) })
	t.Run("ColumnAddedMidTableEarlierCLI", func(t *testing.T) { midTable(t, "m08midold", true) })

	// Review-1 F1: the expected state is read with what applied up files
	// left in place, so those files must be the ones that ran. Editing the
	// applied up file of a destructive migration must not turn real drift
	// into "Database matches" (the reviewer's T2).
	t.Run("EditedAppliedUpFileRefused", func(t *testing.T) {
		dbURL, fx := newM02CommandDB(t, "m08tamper")
		if err := fx.Exec(context.Background(), `CREATE TABLE t (id integer PRIMARY KEY, keep text, old integer)`); err != nil {
			t.Fatal(err)
		}
		work := t.TempDir()
		mig := filepath.Join(work, "migrations")
		must(t, dbURL, "schema", "baseline", "--dir", mig)
		pulled := filepath.Join(work, "pulled.json")
		must(t, dbURL, "schema", "pull", "--out", pulled)
		desired := filepath.Join(work, "d.json")
		writeFile(t, desired, strings.Replace(string(mustReadFile(t, pulled)), `,{"name":"old","notNull":false,"type":{"codec":"number","name":"int4"}}`, "", 1))
		if strings.Contains(string(mustReadFile(t, desired)), `"old"`) {
			t.Fatalf("fixture: old must be gone from the desired document")
		}
		must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", desired, "--name", "drop_old", "--allow-destructive")
		must(t, dbURL, "migrate", "--dir", mig, "--allow-destructive")
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		if err := fx.Exec(context.Background(), `ALTER TABLE t ADD COLUMN old integer`); err != nil {
			t.Fatal(err)
		}
		refused(t, dbURL, []string{"drift", `drop column if exists "old"`}, "schema", "check", "--live", "--dir", mig)
		writeFile(t, filepath.Join(mig, "001_drop_old.up.sql"), "-- Migration: drop_old\n\nselect 1;\n")
		refused(t, dbURL, []string{"modified since they were applied", "001"}, "schema", "check", "--live", "--dir", mig)
		refused(t, dbURL, []string{"modified since they were applied"}, "migrate", "--dir", mig)
	})

	// M08 review-2 finding 1 (P6, P6b): the chain is read as recorded, so
	// an up file edited while generate runs cannot shape the snapshot it
	// writes. Edit 001's up file (applied or pending), generate 002, restore
	// 001 as migrate's refusal says: 002 records no phantom and the chain
	// stays consistent after both apply.
	for _, applied := range []bool{true, false} {
		name := "EditedUpFileDoesNotShapeSnapshot/pending"
		if applied {
			name = "EditedUpFileDoesNotShapeSnapshot/applied"
		}
		t.Run(name, func(t *testing.T) {
			label := "m08p6b"
			if applied {
				label = "m08p6"
			}
			dbURL, fx := newM02CommandDB(t, label)
			if err := fx.Exec(context.Background(), `CREATE TABLE t (id integer PRIMARY KEY, keep text, old integer)`); err != nil {
				t.Fatal(err)
			}
			work := t.TempDir()
			mig := filepath.Join(work, "migrations")
			must(t, dbURL, "schema", "baseline", "--dir", mig)
			pulled := filepath.Join(work, "pulled.json")
			must(t, dbURL, "schema", "pull", "--out", pulled)
			d1, d2 := filepath.Join(work, "d1.json"), filepath.Join(work, "d2.json")
			doc := strings.Replace(string(mustReadFile(t, pulled)), `,{"name":"old","notNull":false,"type":{"codec":"number","name":"int4"}}`, "", 1)
			writeFile(t, d1, doc)
			writeFile(t, d2, strings.Replace(doc, `{"name":"keep","notNull":false,"type":{"codec":"string","name":"text"}}`, `{"name":"keep","notNull":false,"type":{"codec":"string","name":"text"}},{"name":"n2","notNull":false,"type":{"codec":"number","name":"int4"}}`, 1))
			must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", d1, "--name", "drop_old", "--allow-destructive")
			if applied {
				must(t, dbURL, "migrate", "--dir", mig, "--allow-destructive")
			}
			up := filepath.Join(mig, "001_drop_old.up.sql")
			original := string(mustReadFile(t, up))
			writeFile(t, up, "-- Migration: drop_old\n\nselect 1;\n")
			must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", d2, "--name", "add_n2")
			if strings.Contains(string(mustReadFile(t, filepath.Join(mig, "snapshots", "002_add_n2.snapshot.json"))), `"old"`) {
				t.Fatalf("002 records a phantom old read from the edited up file")
			}
			writeFile(t, up, original)
			must(t, dbURL, "schema", "check", "--live", "--dir", mig)
			must(t, dbURL, "migrate", "--dir", mig, "--allow-destructive")
			must(t, dbURL, "schema", "check", "--live", "--dir", mig)
			if got := q09Query(t, fx, m08Columns); got != "id,keep,n2" {
				t.Fatalf("t columns %s", got)
			}
		})
	}

	// M08 review-2 (review INFO 5): the pre-flight refuses a history row
	// without the v2 format marker, as `neutron migrate` does.
	t.Run("HistoryFormatVerified", func(t *testing.T) {
		dbURL, fx, work, mig := setup(t, "m08format")
		must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", filepath.Join(work, "d1.json"), "--name", "add_note")
		must(t, dbURL, "migrate", "--dir", mig)
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		if err := fx.Exec(context.Background(), `UPDATE _neutron_migrations SET format = NULL WHERE version = '001'`); err != nil {
			t.Fatal(err)
		}
		refused(t, dbURL, []string{"001", "without the v2 format marker", "neutron migrate adopt"}, "schema", "check", "--live", "--dir", mig)
	})

	// M08 review-2: a swap of existing columns is informational: noted,
	// nothing planned for it; a new column in the same document is added
	// last and the snapshot records the database order.
	t.Run("SwapOfExistingColumnsIsNoted", func(t *testing.T) {
		dbURL, fx := newM02CommandDB(t, "m08swap")
		if err := fx.Exec(context.Background(), `CREATE TABLE u (id integer PRIMARY KEY, a text, b text)`); err != nil {
			t.Fatal(err)
		}
		work := t.TempDir()
		mig := filepath.Join(work, "migrations")
		pulled := filepath.Join(work, "pulled.json")
		must(t, dbURL, "schema", "baseline", "--dir", mig)
		must(t, dbURL, "schema", "pull", "--out", pulled)
		swap, swapAdd := filepath.Join(work, "swap.json"), filepath.Join(work, "swap_add.json")
		m08ColumnsAs(t, pulled, swap, "id", "b", "a")
		m08ColumnsAs(t, pulled, swapAdd, "id", "b", "mid", "a")
		out := must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", swap, "--name", "swap")
		if !strings.Contains(out, "No schema changes detected") || !strings.Contains(out, "the database holds them as (id, a, b)") {
			t.Fatalf("a swap alone plans nothing and is noted:\n%s", out)
		}
		must(t, unreachable, "schema", "check", "--dir", mig, "--schema", swap)
		must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", swapAdd, "--name", "add_mid")
		if up := string(mustReadFile(t, filepath.Join(mig, "001_add_mid.up.sql"))); !strings.Contains(up, `add column "mid"`) || strings.Count(up, ";") != 1 {
			t.Fatalf("only mid is planned:\n%s", up)
		}
		must(t, dbURL, "migrate", "--dir", mig)
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		must(t, unreachable, "schema", "check", "--dir", mig, "--schema", swapAdd)
		if got := q09Query(t, fx, m08UColumns); got != "id,a,b,mid" {
			t.Fatalf("u columns %s", got)
		}
	})

	// Review-1 F5: a table left in place whose foreign key points into a
	// schema the document does not declare: the refusal names the target
	// to declare, once, and declaring it works.
	t.Run("UndeclaredReferenceNamesTarget", func(t *testing.T) {
		dbURL, fx := newM02CommandDB(t, "m08ref")
		if err := fx.Exec(context.Background(), `CREATE SCHEMA s2; CREATE TABLE s2.x (id integer PRIMARY KEY); CREATE TABLE t (id integer PRIMARY KEY); CREATE TABLE audit (id integer PRIMARY KEY, x_id integer REFERENCES s2.x (id))`); err != nil {
			t.Fatal(err)
		}
		work := t.TempDir()
		mig := filepath.Join(work, "migrations")
		pulled := filepath.Join(work, "pulled.json")
		must(t, dbURL, "schema", "baseline", "--dir", mig)
		must(t, dbURL, "schema", "pull", "--out", pulled)
		shape := func(out string, keepS2 bool) {
			doc, err := db.ParseV2Document(mustReadFile(t, pulled))
			if err != nil {
				t.Fatal(err)
			}
			m, err := db.ModelFromRoot(doc.Root)
			if err != nil {
				t.Fatal(err)
			}
			var tables []db.V2Table
			for _, tb := range m.Tables {
				switch {
				case tb.Identity.Name == "t":
					tb.Columns = append(tb.Columns, db.V2Column{Name: "note", Type: db.V2ColumnType{Name: "int4", Codec: "number"}})
					tables = append(tables, tb)
				case keepS2 && tb.Identity.Schema == "s2":
					tables = append(tables, tb)
				}
			}
			m.Tables = tables
			if !keepS2 {
				m.Schemas = []db.V2SchemaDecl{{Name: "public"}}
			}
			root, err := db.RootFromModel(m)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			writeFile(t, out, string(raw))
		}
		d1, d2 := filepath.Join(work, "d1.json"), filepath.Join(work, "d2.json")
		shape(d1, false)
		shape(d2, true)
		code, out := runCLIProcess(t, bin, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", d1, "--name", "add_note")
		refusal := ""
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, "✗") {
				refusal = line
			}
		}
		if code == 0 || !strings.Contains(refusal, "references table s2.x") || !strings.Contains(refusal, "declare schema s2 and table s2.x in the schema document") || strings.Count(refusal, "--allow-destructive") != 1 {
			t.Fatalf("want one refusal naming s2.x (%d):\n%s", code, out)
		}
		must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", d2, "--name", "add_note")
		must(t, dbURL, "migrate", "--dir", mig)
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
	})

	// An earlier CLI drifted the chain and the user then dropped the objects
	// by hand to get past the drift gate. The chain is read as recorded, so
	// that state matches it and the workflow simply continues.
	t.Run("HandDroppedAfterEarlierCLI", func(t *testing.T) {
		dbURL, fx, work, mig := setup(t, "m08hand")
		m08PreM08Migration(t, mig, "001", "add_note", filepath.Join(work, "d1.json"))
		must(t, dbURL, "migrate", "--dir", mig)
		if err := fx.Exec(context.Background(), `DROP VIEW v_other; DROP TABLE other_app; DROP TYPE mood; ALTER TABLE t DROP COLUMN old`); err != nil {
			t.Fatal(err)
		}
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		must(t, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", filepath.Join(work, "d2.json"), "--name", "add_note2")
		must(t, dbURL, "migrate", "--dir", mig)
		must(t, dbURL, "schema", "check", "--live", "--dir", mig)
		if got := q09Query(t, fx, m08Columns); got != "id,keep,note,note2" {
			t.Fatalf("t columns %s", got)
		}
	})
}
