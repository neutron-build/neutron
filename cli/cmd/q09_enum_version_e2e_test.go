package cmd

// Q09 live coverage through the REAL CLI binary: plans that add an enum
// value and use it in the same change, and version-gated DDL, on the
// product apply paths (db push, migrate generate live/snapshot, migrate).
// Skipped unless NEUTRON_E2E_DATABASE_URL is set (NEUTRON_LIVE_REQUIRED=1
// fails instead).

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// q09DocA: an enum, a table using it, and a stored generated column.
const q09DocA = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "app"}],
	"enums": [
		{"identity": {"schema": "app", "name": "mood"}, "managed": true, "values": ["sad", "ok", "glad"]}
	],
	"tables": [{
		"identity": {"schema": "app", "name": "tenants"},
		"managed": true,
		"columns": [
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			{"name": "tone", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "app", "name": "mood"}}, "notNull": false,
			 "default": {"kind": "literal", "sql": "'ok'::app.mood"}},
			{"name": "net", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false},
			{"name": "gross", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false,
			 "generated": {"expression": "net * 2"}}
		],
		"constraints": [{"name": "tenants_pkey", "type": "primary-key", "columns": ["id"]}],
		"indexes": []
	}],
	"views": [],
	"opaque": []
}`

// q09DocB adds the enum value "elated" and uses it three ways in the same
// change: a column default, a check constraint and a view.
var q09DocB = strings.NewReplacer(
	`"values": ["sad", "ok", "glad"]`, `"values": ["sad", "ok", "glad", "elated"]`,
	`"sql": "'ok'::app.mood"`, `"sql": "'elated'::app.mood"`,
	`"constraints": [{"name": "tenants_pkey", "type": "primary-key", "columns": ["id"]}]`,
	`"constraints": [{"name": "tenants_pkey", "type": "primary-key", "columns": ["id"]},
		{"name": "tenants_net_check", "type": "check", "expression": "net > 0 or tone = 'elated'"}]`,
	`"views": []`,
	`"views": [{"identity": {"schema": "app", "name": "elated_tenants"}, "managed": true,
		"definition": "select id from app.tenants where tone in ('glad', 'elated')"}]`,
).Replace(q09DocA)

// q09DocExpr changes the generated expression (ALTER COLUMN ... SET
// EXPRESSION, PostgreSQL 17+) and adds an unrelated column, so a refused
// plan can be shown to have applied nothing.
var q09DocExpr = strings.NewReplacer(
	`"generated": {"expression": "net * 2"}}`,
	`"generated": {"expression": "net * 3"}},
			{"name": "memo", "type": {"name": "text", "codec": "string"}, "notNull": false}`,
).Replace(q09DocA)

func q09Fixtures(t *testing.T) (work, docA, docB, docExpr string) {
	t.Helper()
	if q09DocB == q09DocA || q09DocExpr == q09DocA || strings.Count(q09DocB, "elated") != 5 {
		t.Fatal("Q09 fixture edits did not apply")
	}
	work = t.TempDir()
	docA = filepath.Join(work, "a.json")
	docB = filepath.Join(work, "b.json")
	docExpr = filepath.Join(work, "expr.json")
	writeFile(t, docA, q09DocA)
	writeFile(t, docB, q09DocB)
	writeFile(t, docExpr, q09DocExpr)
	return work, docA, docB, docExpr
}

func q09Query(t *testing.T, client *db.Client, sql string) string {
	t.Helper()
	var v string
	if err := client.QueryRow(context.Background(), sql).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return v
}

func q09ServerMajor(t *testing.T, client *db.Client) int {
	t.Helper()
	n, err := strconv.Atoi(q09Query(t, client, `SELECT (current_setting('server_version_num')::int / 10000)::text`))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

const q09MoodCount = `SELECT count(*)::text FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid JOIN pg_namespace n ON n.oid = t.typnamespace WHERE t.typname = 'mood' AND n.nspname = 'app'`

const q09ViewCount = `SELECT count(*)::text FROM pg_views WHERE schemaname = 'app' AND viewname = 'elated_tenants'`

const q09GrossExpr = `SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid = 'app.tenants'::regclass AND adnum = (SELECT attnum FROM pg_attribute WHERE attrelid = 'app.tenants'::regclass AND attname = 'gross')`

// A1 on db push: a plan that adds an enum value and uses it applies in two
// explicitly reported transactions, never as one transaction PostgreSQL
// rejects (55P04) and never as a silent split.
func TestQ09PushEnumAdditionUsedBySamePlan(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "q09push")
	bin := buildCLIBinary(t)
	_, docA, docB, _ := q09Fixtures(t)
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	if code, out := run("db", "push", "--schema", docA); code != 0 {
		t.Fatalf("push A failed (%d):\n%s", code, out)
	}

	// The dry run shows the phase boundary before anything is applied.
	code, out := run("db", "push", "--schema", docB, "--dry-run")
	if code != 0 {
		t.Fatalf("dry run failed (%d):\n%s", code, out)
	}
	p1, p2 := strings.Index(out, "-- phase 1 of 2"), strings.Index(out, "-- phase 2 of 2")
	add, view := strings.Index(out, "add value 'elated'"), strings.Index(out, "create view")
	if p1 < 0 || p2 < 0 || !(p1 < add && add < p2 && p2 < view) {
		t.Fatalf("dry run must show the enum addition as phase 1 and its uses as phase 2:\n%s", out)
	}
	if got := q09Query(t, fx, q09MoodCount); got != "3" {
		t.Fatalf("dry run must not apply anything (mood has %s values)", got)
	}
	// A table element that uses the new value cannot be normalized against
	// the live type; its other expressions still must be (no spurious
	// generated-column rewrite, which older servers would also refuse).
	if strings.Contains(out, "set expression") {
		t.Fatalf("the unchanged generated expression must not be re-planned:\n%s", out)
	}

	// Phase 2 fails (an existing row violates the new check): phase 1 stays
	// committed and the output says so; phase 2 rolls back as a whole.
	if err := fx.Exec(context.Background(), `INSERT INTO app.tenants (id, tone, net) VALUES (1, 'ok', -1)`); err != nil {
		t.Fatal(err)
	}
	code, out = run("db", "push", "--schema", docB)
	if code == 0 {
		t.Fatalf("push must fail when phase 2 fails:\n%s", out)
	}
	for _, want := range []string{"55P04", "phase 1 of 2", "committed", "phase 2 of 2", "rolled back"} {
		if !strings.Contains(out, want) {
			t.Fatalf("phase-2 failure report must mention %q:\n%s", want, out)
		}
	}
	if got := q09Query(t, fx, q09MoodCount); got != "4" {
		t.Fatalf("phase 1 (enum addition) must stay committed, mood has %s values", got)
	}
	if got := q09Query(t, fx, q09ViewCount); got != "0" {
		t.Fatalf("phase 2 must roll back as a whole (view count %s)", got)
	}

	// Fix the data and push again: the value exists, so the plan is one
	// transaction; it converges.
	if err := fx.Exec(context.Background(), `UPDATE app.tenants SET net = 1`); err != nil {
		t.Fatal(err)
	}
	code, out = run("db", "push", "--schema", docB)
	if code != 0 {
		t.Fatalf("push after the data fix failed (%d):\n%s", code, out)
	}
	if strings.Contains(out, "phase 1 of 2") {
		t.Fatalf("with the value already present the plan is a single transaction:\n%s", out)
	}

	// A clean database takes both phases in one successful push.
	dbURL2, fx2 := newM02CommandDB(t, "q09push2")
	if code, out := runCLIProcess(t, bin, dbURL2, "db", "push", "--schema", docA); code != 0 {
		t.Fatalf("push A failed (%d):\n%s", code, out)
	}
	code, out = runCLIProcess(t, bin, dbURL2, "db", "push", "--schema", docB)
	if code != 0 {
		t.Fatalf("push B must apply the enum addition and its uses (%d):\n%s", code, out)
	}
	for _, want := range []string{"phase 1 of 2", "55P04", "phase 2 of 2"} {
		if !strings.Contains(out, want) {
			t.Fatalf("push output must report the phases (%q missing):\n%s", want, out)
		}
	}
	if got := q09Query(t, fx2, q09MoodCount); got != "4" {
		t.Fatalf("mood must have 4 values, has %s", got)
	}
	if got := q09Query(t, fx2, `SELECT count(*)::text FROM app.elated_tenants`); got != "0" {
		t.Fatalf("view must be queryable, got %s", got)
	}
	if got := q09Query(t, fx2, `SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid = 'app.tenants'::regclass AND adnum = (SELECT attnum FROM pg_attribute WHERE attrelid = 'app.tenants'::regclass AND attname = 'tone')`); !strings.Contains(got, "elated") {
		t.Fatalf("tone default must use the new value, got %q", got)
	}
	code, out = runCLIProcess(t, bin, dbURL2, "db", "push", "--schema", docB)
	if code != 0 || !strings.Contains(out, "in sync") {
		t.Fatalf("second push must converge (%d):\n%s", code, out)
	}
}

// A1 on migrate generate (live mode) + migrate: the enum addition is its
// own earlier migration, so every generated file applies.
func TestQ09GenerateLiveSplitsEnumAdditions(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "q09genlive")
	bin := buildCLIBinary(t)
	work, docA, docB, _ := q09Fixtures(t)
	mig := filepath.Join(work, "mig")
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	if code, out := run("migrate", "generate", "--mode", "live", "--schema", docA, "--dir", mig, "--name", "init"); code != 0 {
		t.Fatalf("generate A failed (%d):\n%s", code, out)
	}
	if code, out := run("migrate", "--dir", mig); code != 0 {
		t.Fatalf("migrate A failed (%d):\n%s", code, out)
	}

	code, out := run("migrate", "generate", "--mode", "live", "--schema", docB, "--dir", mig, "--name", "mood")
	if code != 0 {
		t.Fatalf("generate B failed (%d):\n%s", code, out)
	}
	if !strings.Contains(out, "55P04") || !strings.Contains(out, "(002_mood_enum_values)") {
		t.Fatalf("generate must say why the enum addition is its own migration, named by its file:\n%s", out)
	}
	enumUp := readFile(t, filepath.Join(mig, "002_mood_enum_values.up.sql"))
	restUp := readFile(t, filepath.Join(mig, "003_mood.up.sql"))
	if !strings.Contains(enumUp, "add value 'elated'") || strings.Contains(enumUp, "create view") || strings.Contains(enumUp, "check") {
		t.Fatalf("002 must carry only the enum addition:\n%s", enumUp)
	}
	if strings.Contains(restUp, "add value") || !strings.Contains(restUp, "create view") {
		t.Fatalf("003 must carry the uses, not the addition:\n%s", restUp)
	}

	code, out = run("migrate", "--dir", mig)
	if code != 0 {
		t.Fatalf("migrate must apply both generated migrations (%d):\n%s", code, out)
	}
	if got := q09Query(t, fx, `SELECT string_agg(version, ',' ORDER BY version) FROM _neutron_migrations`); got != "001,002,003" {
		t.Fatalf("history = %s, want 001,002,003", got)
	}
	if got := q09Query(t, fx, q09ViewCount); got != "1" {
		t.Fatal("the view using the new value must exist")
	}
	// Converged: nothing left to generate (the history table outside the
	// document's schemas is only reported).
	code, out = run("migrate", "generate", "--mode", "live", "--schema", docB, "--dir", mig, "--name", "again")
	if code != 0 || strings.Contains(out, "Generated") || !strings.Contains(out, "No applicable changes") {
		t.Fatalf("the database must converge to B (%d):\n%s", code, out)
	}
	if entries, _ := os.ReadDir(mig); len(entries) != 6 {
		t.Fatalf("convergence must not write a migration; %s has %d entries", mig, len(entries))
	}
}

// A1 on migrate generate (snapshot mode) + migrate: two migrations with
// their own plan reports and snapshots; the chain stays continuous.
func TestQ09GenerateSnapshotSplitsEnumAdditions(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "q09gensnap")
	bin := buildCLIBinary(t)
	work, docA, docB, _ := q09Fixtures(t)
	mig := filepath.Join(work, "mig")
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	if code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", docA, "--dir", mig, "--name", "init"); code != 0 {
		t.Fatalf("generate A failed (%d):\n%s", code, out)
	}
	if code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", docB, "--dir", mig, "--name", "mood"); code != 0 {
		t.Fatalf("generate B failed (%d):\n%s", code, out)
	}
	for _, f := range []string{"002_mood_enum_values.up.sql", "002_mood_enum_values.plan.json", "003_mood.up.sql", "003_mood.plan.json",
		filepath.Join("snapshots", "002_mood_enum_values.snapshot.json"), filepath.Join("snapshots", "003_mood.snapshot.json")} {
		if _, err := os.Stat(filepath.Join(mig, f)); err != nil {
			t.Fatalf("snapshot generate must write %s: %v", f, err)
		}
	}
	if plan := readFile(t, filepath.Join(mig, "003_mood.plan.json")); !strings.Contains(plan, `"baseSource": "002_mood_enum_values"`) {
		t.Fatalf("003 must plan from 002's snapshot:\n%s", plan)
	}

	if code, out := run("migrate", "--dir", mig); code != 0 {
		t.Fatalf("migrate must apply all three migrations (%d):\n%s", code, out)
	}
	if got := q09Query(t, fx, q09ViewCount); got != "1" {
		t.Fatal("the view using the new value must exist")
	}
	if code, out := run("schema", "check", "--live", "--dir", mig); code != 0 {
		t.Fatalf("live check after apply failed (%d):\n%s", code, out)
	}
	code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", docB, "--dir", mig, "--name", "again")
	if code != 0 || !strings.Contains(out, "No schema changes") {
		t.Fatalf("the chain head must equal B (%d):\n%s", code, out)
	}
}

// A1 on migrate with an existing single file that adds and uses a value:
// an actionable refusal naming the fix, with the migration rolled back.
func TestQ09MigrateCombinedEnumFileFailsActionably(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "q09combined")
	bin := buildCLIBinary(t)
	mig := filepath.Join(t.TempDir(), "mig")
	if err := os.MkdirAll(mig, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(mig, "001_init.up.sql"), `create schema app;
create type app.mood as enum ('sad', 'ok', 'glad');
create table app.tenants (id int primary key, tone app.mood);`)
	writeFile(t, filepath.Join(mig, "002_mood.up.sql"), `alter type app.mood add value 'elated';
create view app.elated_tenants as select id from app.tenants where tone = 'elated';`)

	code, out := runCLIProcess(t, bin, dbURL, "migrate", "--dir", mig)
	if code == 0 {
		t.Fatalf("a migration that uses the enum value it adds cannot apply:\n%s", out)
	}
	for _, want := range []string{"002_mood", "55P04", "own earlier migration", "migrate generate"} {
		if !strings.Contains(out, want) {
			t.Fatalf("the refusal must be actionable (%q missing):\n%s", want, out)
		}
	}
	if got := q09Query(t, fx, `SELECT string_agg(version, ',' ORDER BY version) FROM _neutron_migrations`); got != "001" {
		t.Fatalf("history = %s, want only 001", got)
	}
	if got := q09Query(t, fx, q09MoodCount); got != "3" {
		t.Fatalf("002 must roll back as a whole (mood has %s values)", got)
	}
}

// A2: ALTER COLUMN ... SET EXPRESSION is PostgreSQL 17+. Live planning
// refuses it up front on older servers; snapshot plans record the
// requirement and migrate checks it before running anything.
func TestQ09GeneratedExpressionServerVersionGate(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "q09expr")
	bin := buildCLIBinary(t)
	work, docA, _, docExpr := q09Fixtures(t)
	major := q09ServerMajor(t, fx)
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	if code, out := run("db", "push", "--schema", docA); code != 0 {
		t.Fatalf("push A failed (%d):\n%s", code, out)
	}

	liveMig := filepath.Join(work, "live")
	code, out := run("db", "push", "--schema", docExpr)
	genCode, genOut := run("migrate", "generate", "--mode", "live", "--schema", docExpr, "--dir", liveMig, "--name", "expr")
	if major >= 17 {
		if code != 0 {
			t.Fatalf("PostgreSQL %d supports SET EXPRESSION; push failed (%d):\n%s", major, code, out)
		}
		if got := q09Query(t, fx, q09GrossExpr); !strings.Contains(got, "3") {
			t.Fatalf("expression must change, got %q", got)
		}
		if genCode != 0 || !strings.Contains(genOut, "No schema changes") {
			t.Fatalf("live generate after the push must find nothing (%d):\n%s", genCode, genOut)
		}
	} else {
		if code == 0 {
			t.Fatalf("PostgreSQL %d cannot run SET EXPRESSION; push must refuse:\n%s", major, out)
		}
		for _, want := range []string{"PostgreSQL 17", "PostgreSQL " + strconv.Itoa(major), "gross", "--allow-destructive"} {
			if !strings.Contains(out, want) {
				t.Fatalf("push refusal must be actionable (%q missing):\n%s", want, out)
			}
		}
		if strings.Contains(out, "applied:") {
			t.Fatalf("a refused plan must not apply any statement:\n%s", out)
		}
		if got := q09Query(t, fx, `SELECT count(*)::text FROM pg_attribute WHERE attrelid = 'app.tenants'::regclass AND attname = 'memo'`); got != "0" {
			t.Fatal("the refused plan's other changes must not apply")
		}
		if got := q09Query(t, fx, q09GrossExpr); !strings.Contains(got, "2") {
			t.Fatalf("expression must be unchanged, got %q", got)
		}
		if genCode == 0 || !strings.Contains(genOut, "PostgreSQL 17") {
			t.Fatalf("live generate must refuse on PostgreSQL %d (%d):\n%s", major, genCode, genOut)
		}
		if _, err := os.Stat(liveMig); err == nil {
			if entries, _ := os.ReadDir(liveMig); len(entries) != 0 {
				t.Fatalf("a refused live generate must write nothing, found %d entries", len(entries))
			}
		}
	}

	// Snapshot mode: offline planning cannot know the target server; the
	// plan records the requirement and migrate enforces it.
	snapMig := filepath.Join(work, "snap")
	if code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", docA, "--dir", snapMig, "--name", "init"); code != 0 {
		t.Fatalf("snapshot generate A failed (%d):\n%s", code, out)
	}
	if code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", docExpr, "--dir", snapMig, "--name", "expr"); code != 0 {
		t.Fatalf("snapshot generate expr failed (%d):\n%s", code, out)
	}
	plan := readFile(t, filepath.Join(snapMig, "002_expr.plan.json"))
	if !strings.Contains(plan, `"minServerMajor": 17`) {
		t.Fatalf("plan.json must record the PostgreSQL 17 requirement:\n%s", plan)
	}

	dbURL2, fx2 := newM02CommandDB(t, "q09exprsnap")
	code, out = runCLIProcess(t, bin, dbURL2, "migrate", "--dir", snapMig)
	if major >= 17 {
		if code != 0 {
			t.Fatalf("migrate on PostgreSQL %d failed (%d):\n%s", major, code, out)
		}
		if got := q09Query(t, fx2, q09GrossExpr); !strings.Contains(got, "3") {
			t.Fatalf("expression must change, got %q", got)
		}
		return
	}
	if code == 0 {
		t.Fatalf("migrate must refuse a plan requiring PostgreSQL 17 on PostgreSQL %d:\n%s", major, out)
	}
	for _, want := range []string{"002_expr", "PostgreSQL 17", "PostgreSQL " + strconv.Itoa(major)} {
		if !strings.Contains(out, want) {
			t.Fatalf("migrate refusal must name the requirement (%q missing):\n%s", want, out)
		}
	}
	if got := q09Query(t, fx2, `SELECT count(*)::text FROM _neutron_migrations`); got != "0" {
		t.Fatalf("the refusal happens before any migration runs; history has %s rows", got)
	}
	if got := q09Query(t, fx2, `SELECT (to_regclass('app.tenants') IS NULL)::text`); got != "true" {
		t.Fatal("no statement of the batch may run")
	}
}

// q09DocR1 / q09DocR2 are review-1's r1.json / r2.json: r2 adds a new enum
// type and a column of that type to a table that has a generated column,
// a check and an expression index, all unchanged.
const q09DocR1 = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "app"}],
	"enums": [
		{"identity": {"schema": "app", "name": "mood"}, "managed": true, "values": ["sad", "ok", "glad"]},
		{"identity": {"schema": "app", "name": "color"}, "managed": true, "values": ["red", "blue"]}
	],
	"tables": [{
		"identity": {"schema": "app", "name": "tenants"},
		"managed": true,
		"columns": [
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			{"name": "tone", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "app", "name": "mood"}}, "notNull": false,
			 "default": {"kind": "literal", "sql": "'ok'::app.mood"}},
			{"name": "net", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false},
			{"name": "gross", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false,
			 "generated": {"expression": "net * 2"}}
		],
		"constraints": [
			{"name": "tenants_pkey", "type": "primary-key", "columns": ["id"]},
			{"name": "tenants_net_pos", "type": "check", "expression": "net > 0"}
		],
		"indexes": [{"identity": {"schema": "app", "name": "tenants_lnet_idx"}, "unique": false, "method": "btree",
			"key": [{"expression": "abs(net)"}], "where": "net > 1"}]
	}],
	"views": [],
	"opaque": []
}`

var q09DocR2 = strings.NewReplacer(
	`"values": ["red", "blue"]}`,
	`"values": ["red", "blue"]},
		{"identity": {"schema": "app", "name": "size"}, "managed": true, "values": ["s", "m", "l"]}`,
	`"generated": {"expression": "net * 2"}}`,
	`"generated": {"expression": "net * 2"}},
			{"name": "size", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "app", "name": "size"}}, "notNull": false}`,
).Replace(q09DocR1)

// Review-1 finding 1: a column typed by an enum the same change creates
// made the table's whole twin fail, so its unchanged generated expression,
// check and index were compared as raw text — a false SET EXPRESSION
// refusal on PostgreSQL 16, and on 17+ a table rewrite plus check and
// index churn. The plan must be just the new type and column.
func TestQ09PushNewEnumColumnKeepsUnchangedExpressions(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "q09newenumcol")
	bin := buildCLIBinary(t)
	work := t.TempDir()
	r1, r2 := filepath.Join(work, "r1.json"), filepath.Join(work, "r2.json")
	if q09DocR2 == q09DocR1 || strings.Count(q09DocR2, `"size"`) != 3 {
		t.Fatal("r2 fixture edits did not apply")
	}
	writeFile(t, r1, q09DocR1)
	writeFile(t, r2, q09DocR2)
	major := q09ServerMajor(t, fx)
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	if code, out := run("db", "push", "--schema", r1); code != 0 {
		t.Fatalf("push r1 failed (%d):\n%s", code, out)
	}
	const identity = `SELECT format('%s/%s/%s',
		(SELECT relfilenode FROM pg_class WHERE oid = 'app.tenants'::regclass),
		(SELECT oid FROM pg_constraint WHERE conrelid = 'app.tenants'::regclass AND conname = 'tenants_net_pos'),
		(SELECT relfilenode FROM pg_class WHERE oid = 'app.tenants_lnet_idx'::regclass))`
	before := q09Query(t, fx, identity)

	code, out := run("db", "push", "--schema", r2, "--dry-run")
	if code != 0 {
		t.Fatalf("PostgreSQL %d: the dry run must plan, not refuse (%d):\n%s", major, code, out)
	}
	for _, bad := range []string{"set expression", "SET EXPRESSION", "drop constraint", "drop index", "equivalence not verified"} {
		if strings.Contains(out, bad) {
			t.Fatalf("PostgreSQL %d: unchanged expressions must compare equal (%q in the plan):\n%s", major, bad, out)
		}
	}
	for _, want := range []string{`create type "app"."size"`, `add column "size"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("the plan must carry %q:\n%s", want, out)
		}
	}

	code, out = run("db", "push", "--schema", r2)
	if code != 0 {
		t.Fatalf("PostgreSQL %d: push r2 failed (%d):\n%s", major, code, out)
	}
	if after := q09Query(t, fx, identity); after != before {
		t.Fatalf("PostgreSQL %d: no table rewrite, check re-add or index rebuild expected (table/check/index %s -> %s):\n%s", major, before, after, out)
	}
	if code, out := run("db", "push", "--schema", r2, "--dry-run"); code != 0 || !strings.Contains(out, "in sync") {
		t.Fatalf("r2 must converge (%d):\n%s", code, out)
	}
}

// Review-2 finding 1: a new-enum column with a default (its default cannot
// be normalized before the type exists) made the whole table unverified,
// so a real change to the generated expression was refused on
// PostgreSQL 16 as "could not be verified ... may be spelling only" and
// planned on 17+ with a false "equivalence not verified" warning. Only an
// element that actually failed may be unverified: gross rewritten to use
// the new column keeps the honest wording.
func TestQ09NewEnumColumnUnverifiesOnlyFailedElements(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "q09scopedunverified")
	bin := buildCLIBinary(t)
	work := t.TempDir()
	r1, v2, v4b := filepath.Join(work, "r1.json"), filepath.Join(work, "v2.json"), filepath.Join(work, "v4b.json")
	newSize := func(expr, sizeColumn string) string {
		return strings.NewReplacer(
			`"values": ["red", "blue"]}`,
			`"values": ["red", "blue"]},
		{"identity": {"schema": "app", "name": "size"}, "managed": true, "values": ["s", "m", "l"]}`,
			`"generated": {"expression": "net * 2"}}`,
			`"generated": {"expression": "`+expr+`"}},
			`+sizeColumn,
		).Replace(q09DocR1)
	}
	writeFile(t, r1, q09DocR1)
	writeFile(t, v2, newSize("net * 3", `{"name": "size", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "app", "name": "size"}}, "notNull": true,
			 "default": {"kind": "literal", "sql": "'m'::app.size"}}`))
	writeFile(t, v4b, newSize("net * case when size = 's' then 1 else 2 end", `{"name": "size", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "app", "name": "size"}}, "notNull": false}`))
	major := q09ServerMajor(t, fx)
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}
	if code, out := run("db", "push", "--schema", r1); code != 0 {
		t.Fatalf("push r1 failed (%d):\n%s", code, out)
	}

	code, out := run("db", "push", "--schema", v2, "--dry-run")
	if major < 17 {
		if code == 0 || !strings.Contains(out, `generated column "gross" changes its expression`) {
			t.Fatalf("PostgreSQL %d: the verified change must be refused as a change (%d):\n%s", major, code, out)
		}
		for _, bad := range []string{"could not be verified", "may be spelling only", "equivalence not verified"} {
			if strings.Contains(out, bad) {
				t.Fatalf("PostgreSQL %d: the size default's failure must not unverify gross (%q):\n%s", major, bad, out)
			}
		}
	} else {
		if code != 0 || !strings.Contains(out, `alter column "gross" set expression as ((net * (3)::numeric))`) {
			t.Fatalf("PostgreSQL %d: the dry run must plan SET EXPRESSION (%d):\n%s", major, code, out)
		}
		if strings.Contains(out, "equivalence not verified") {
			t.Fatalf("PostgreSQL %d: no element here is unverified:\n%s", major, out)
		}
	}

	code, out = run("db", "push", "--schema", v4b, "--dry-run")
	if major < 17 {
		if code == 0 || !strings.Contains(out, `generated column "gross" could not be verified`) ||
			!strings.Contains(out, "equivalence not verified for column gross generation expression") {
			t.Fatalf("PostgreSQL %d: gross references the new column and keeps the unverified wording (%d):\n%s", major, code, out)
		}
	} else if code != 0 || !strings.Contains(out, "equivalence not verified for column gross generation expression") {
		t.Fatalf("PostgreSQL %d: gross references the new column and stays flagged unverified (%d):\n%s", major, code, out)
	}
}

// Review-1 finding 2: `migrate resolve --retry` and `--abort` run SQL too,
// so they refuse a server below the migration's floor before anything
// runs, exactly as `migrate` does (resolve accepts a never-attempted
// pending migration, so this is reachable without a server downgrade).
// The plan's recorded floor is raised above the connected server so the
// retry refusal is exercised on every server; the abort refusal (the down
// SQL's own SET EXPRESSION) needs a server older than 17.
func TestQ09ResolveHonorsServerFloor(t *testing.T) {
	dbURL, fx := newM02CommandDB(t, "q09resolvefloor")
	bin := buildCLIBinary(t)
	work, docA, _, docExpr := q09Fixtures(t)
	major := q09ServerMajor(t, fx)
	mig := filepath.Join(work, "snap")
	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	if code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", docA, "--dir", mig, "--name", "init"); code != 0 {
		t.Fatalf("snapshot generate A failed (%d):\n%s", code, out)
	}
	if code, out := run("migrate", "generate", "--mode", "snapshot", "--schema", docExpr, "--dir", mig, "--name", "expr"); code != 0 {
		t.Fatalf("snapshot generate expr failed (%d):\n%s", code, out)
	}
	planPath := filepath.Join(mig, "002_expr.plan.json")
	plan, err := db.LoadPlanArtifact(planPath)
	if err != nil {
		t.Fatal(err)
	}
	floor := major + 1
	plan.MinServerMajor = floor
	raw, err := db.MarshalPlanJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, planPath, string(raw))

	if code, out := run("migrate", "resolve", "001", "--retry", "--dir", mig); code != 0 {
		t.Fatalf("resolve 001 --retry must apply the ungated migration (%d):\n%s", code, out)
	}
	if code, out := run("migrate", "--dir", mig); code == 0 || !strings.Contains(out, "002_expr") {
		t.Fatalf("migrate must refuse 002 below its floor (%d):\n%s", code, out)
	}

	code, out := run("migrate", "resolve", "002", "--retry", "--dir", mig)
	if code == 0 {
		t.Fatalf("resolve --retry must refuse a server below the migration's floor:\n%s", out)
	}
	for _, want := range []string{"002_expr", "PostgreSQL " + strconv.Itoa(floor) + "+", "PostgreSQL " + strconv.Itoa(major), "before any statement runs"} {
		if !strings.Contains(out, want) {
			t.Fatalf("resolve --retry refusal must mention %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "Retrying") || strings.Contains(out, "SQLSTATE") {
		t.Fatalf("the refusal happens before the migration runs:\n%s", out)
	}
	if got := q09Query(t, fx, `SELECT string_agg(version, ',' ORDER BY version) FROM _neutron_migrations`); got != "001" {
		t.Fatalf("history = %s, want only 001", got)
	}
	if got := q09Query(t, fx, `SELECT count(*)::text FROM pg_attribute WHERE attrelid = 'app.tenants'::regclass AND attname = 'memo'`); got != "0" {
		t.Fatal("no statement of 002 may run")
	}

	if major >= 17 {
		return
	}
	code, out = run("migrate", "resolve", "002", "--abort", "--dir", mig)
	if code == 0 {
		t.Fatalf("resolve --abort must refuse a down SQL the server cannot run:\n%s", out)
	}
	for _, want := range []string{"cannot abort 002", "down SQL", "SET EXPRESSION needs PostgreSQL 17+", "before any statement runs"} {
		if !strings.Contains(out, want) {
			t.Fatalf("resolve --abort refusal must mention %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "undone:") || strings.Contains(out, "SQLSTATE") {
		t.Fatalf("the refusal happens before the down SQL runs:\n%s", out)
	}
	if got := q09Query(t, fx, q09GrossExpr); !strings.Contains(got, "2") {
		t.Fatalf("expression must be unchanged, got %q", got)
	}
}
