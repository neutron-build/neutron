package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTestFile(path string, content []byte) error {
	return os.WriteFile(path, content, 0o644)
}

func TestSplitSQLStatements(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"simple", "CREATE TABLE a (id int);\nCREATE TABLE b (id int);",
			[]string{"CREATE TABLE a (id int)", "CREATE TABLE b (id int)"}},
		{"semicolon in string", "INSERT INTO t VALUES ('a;b');\nSELECT 1;",
			[]string{"INSERT INTO t VALUES ('a;b')", "SELECT 1"}},
		{"escaped quote in string", "INSERT INTO t VALUES ('it''s');",
			[]string{"INSERT INTO t VALUES ('it''s')"}},
		{"quoted identifier with dot", `DROP TABLE "my.table";`,
			[]string{`DROP TABLE "my.table"`}},
		{"line comment", "-- header\nCREATE TABLE a (id int); -- trailing\n",
			[]string{"-- header", "CREATE TABLE a (id int)", "-- trailing"}},
		{"block comment with semicolon", "/* a;b */ CREATE TABLE a (id int);",
			[]string{"/* a;b */ CREATE TABLE a (id int)"}},
		{"nested block comment with semicolon", "/* outer /* a;b */ still comment */ CREATE TABLE a (id int);",
			[]string{"/* outer /* a;b */ still comment */ CREATE TABLE a (id int)"}},
		{"comment between create and table", "CREATE /* split */ TABLE a (id int);",
			[]string{"CREATE /* split */ TABLE a (id int)"}},
		{"e-string with escaped quote and semicolon", `SELECT E'a\';b';`,
			[]string{`SELECT E'a\';b'`}},
		{"dollar quote body", "CREATE FUNCTION f() RETURNS void AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql;\nSELECT 1;",
			[]string{"CREATE FUNCTION f() RETURNS void AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql", "SELECT 1"}},
		{"tagged dollar quote", "SELECT quote_literal($tag$;$$;$tag$);",
			[]string{"SELECT quote_literal($tag$;$$;$tag$)"}},
		{"nested dollar tags do not close early", "SELECT $a$ uses $b$ inside $a$;",
			[]string{"SELECT $a$ uses $b$ inside $a$"}},
		{"no trailing semicolon", "CREATE TABLE a (id int)",
			[]string{"CREATE TABLE a (id int)"}},
		{"empty", ";\n;  \n", nil},
		{"statement starting with semicolon spacing", "  ; CREATE VIEW v AS SELECT 1 ; ",
			[]string{"CREATE VIEW v AS SELECT 1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SplitSQLStatements(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("SplitSQLStatements(%q) = %q, want %q", tc.in, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("statement %d = %q, want %q", i+1, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestClassifyStatementRisk(t *testing.T) {
	cases := []struct {
		sql         string
		destructive bool
		dataLoss    bool
	}{
		{"CREATE TABLE a (id int)", false, false},
		{"DROP TABLE a", true, true},
		{"drop index if exists \"Idx\"", true, false},
		{"ALTER TABLE a DROP COLUMN b", true, true},
		{"ALTER TABLE a DROP CONSTRAINT pk", true, false},
		{"ALTER TABLE a ADD COLUMN b int", false, false},
		{"ALTER TABLE a ALTER COLUMN b TYPE text", false, true},
		{"DROP SCHEMA s CASCADE", true, true},
		{"CREATE INDEX CONCURRENTLY i ON t(c)", false, false},
	}
	for _, tc := range cases {
		d, l := ClassifyStatementRisk(tc.sql)
		if d != tc.destructive || l != tc.dataLoss {
			t.Errorf("ClassifyStatementRisk(%q) = (%v,%v), want (%v,%v)", tc.sql, d, l, tc.destructive, tc.dataLoss)
		}
	}
}

func TestIsNontransactionalStatement(t *testing.T) {
	yes := []string{
		"CREATE INDEX CONCURRENTLY i ON t (c)",
		"create unique index concurrently i on t (c)",
		"DROP INDEX CONCURRENTLY i",
		"REINDEX CONCURRENTLY INDEX i",
		"reindex (verbose) table t concurrently",
	}
	no := []string{
		"CREATE INDEX i ON t (c)",
		"DROP INDEX i",
		"CREATE TABLE t (id int)",
		"SELECT 'create index concurrently' -- a string, not a statement head",
	}
	for _, s := range yes {
		if !IsNontransactionalStatement(s) {
			t.Errorf("IsNontransactionalStatement(%q) = false, want true", s)
		}
	}
	for _, s := range no {
		if IsNontransactionalStatement(s) {
			t.Errorf("IsNontransactionalStatement(%q) = true, want false", s)
		}
	}
}

func TestChangesRowData(t *testing.T) {
	yes := []string{
		"INSERT INTO t VALUES (1)",
		"update t set a = 1",
		"DELETE FROM t",
		"MERGE INTO t USING s ON true WHEN MATCHED THEN DO NOTHING",
		"TRUNCATE t",
		"TRUNCATE TABLE t, u",
		"WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
		"COPY t FROM stdin",
		"DO $$ BEGIN NULL; END $$",
		"CALL p()",
	}
	no := []string{
		"CREATE TABLE t (id int)",
		"SELECT pg_sleep(3)",
		"CREATE INDEX CONCURRENTLY i ON t (c)",
		"ALTER TABLE t ADD COLUMN c int",
		"GRANT SELECT ON t TO public",
	}
	for _, s := range yes {
		if !ChangesRowData(s) {
			t.Errorf("ChangesRowData(%q) = false, want true", s)
		}
	}
	for _, s := range no {
		if ChangesRowData(s) {
			t.Errorf("ChangesRowData(%q) = true, want false", s)
		}
	}
}

func TestGuardStatementTargets(t *testing.T) {
	gt := func(name QualifiedName, form, kind string, cascade bool) GuardTarget {
		return GuardTarget{Name: name, Form: form, Kind: kind, Cascade: cascade}
	}
	cases := []struct {
		sql  string
		want []GuardTarget
	}{
		// Pass-1 vocabulary (unchanged behavior, new shape).
		{sql: "DROP TABLE users", want: []GuardTarget{gt(QualifiedName{Name: "users"}, GuardFormDrop, "table", false)}},
		{sql: `drop table if exists "public"."users" cascade`, want: []GuardTarget{gt(QualifiedName{Schema: "public", Name: "users"}, GuardFormDrop, "table", true)}},
		{sql: "DROP TABLE a, b, app.c", want: []GuardTarget{
			gt(QualifiedName{Name: "a"}, GuardFormDrop, "table", false),
			gt(QualifiedName{Name: "b"}, GuardFormDrop, "table", false),
			gt(QualifiedName{Schema: "app", Name: "c"}, GuardFormDrop, "table", false)}},
		{sql: "DROP VIEW IF EXISTS v RESTRICT", want: []GuardTarget{gt(QualifiedName{Name: "v"}, GuardFormDrop, "view", false)}},
		{sql: "DROP INDEX concurrently i1", want: []GuardTarget{gt(QualifiedName{Name: "i1"}, GuardFormDrop, "index", false)}},
		{sql: "DROP TYPE app.status", want: []GuardTarget{gt(QualifiedName{Schema: "app", Name: "status"}, GuardFormDrop, "type", false)}},
		{sql: "DROP SCHEMA IF EXISTS s", want: []GuardTarget{gt(QualifiedName{Name: "s"}, GuardFormDrop, "schema", false)}},
		{sql: "ALTER TABLE app.users DROP COLUMN email", want: []GuardTarget{gt(QualifiedName{Schema: "app", Name: "users"}, GuardFormAlter, "table", false)}},
		{sql: `alter table "Mixed" drop constraint pk`, want: []GuardTarget{gt(QualifiedName{Name: "Mixed"}, GuardFormAlter, "table", false)}},
		{sql: "TRUNCATE TABLE big", want: []GuardTarget{gt(QualifiedName{Name: "big"}, GuardFormTruncate, "table", false)}},
		{sql: "TRUNCATE a, b", want: []GuardTarget{
			gt(QualifiedName{Name: "a"}, GuardFormTruncate, "table", false),
			gt(QualifiedName{Name: "b"}, GuardFormTruncate, "table", false)}},
		{sql: "TRUNCATE _neutron_migrations", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_migrations"}, GuardFormTruncate, "table", false)}},

		// MAJOR-1: widened drop-kind vocabulary.
		{sql: "DROP MATERIALIZED VIEW _neutron_probe_mv", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_probe_mv"}, GuardFormDrop, "materialized-view", false)}},
		{sql: "DROP SEQUENCE _neutron_probe_seq", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_probe_seq"}, GuardFormDrop, "sequence", false)}},
		{sql: "DROP EXTENSION cube CASCADE", want: []GuardTarget{gt(QualifiedName{Name: "cube"}, GuardFormDrop, "extension", true)}},
		{sql: "DROP EXTENSION IF EXISTS cube", want: []GuardTarget{gt(QualifiedName{Name: "cube"}, GuardFormDrop, "extension", false)}},
		{sql: "DROP DOMAIN _neutron_d", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_d"}, GuardFormDrop, "domain", false)}},
		{sql: "DROP FUNCTION _neutron_f(int, text)", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_f"}, GuardFormDrop, "function", false)}},
		{sql: "DROP FUNCTION f(int, text), g(bool)", want: []GuardTarget{
			gt(QualifiedName{Name: "f"}, GuardFormDrop, "function", false),
			gt(QualifiedName{Name: "g"}, GuardFormDrop, "function", false)}},
		{sql: "DROP PROCEDURE app.p()", want: []GuardTarget{gt(QualifiedName{Schema: "app", Name: "p"}, GuardFormDrop, "procedure", false)}},
		// Review-6 BLOCKER-1: the ON table of policy/trigger/rule drops
		// is an alteration target of its own.
		{sql: "DROP POLICY p ON app_t", want: []GuardTarget{
			gt(QualifiedName{Name: "p"}, GuardFormDrop, "policy", false),
			gt(QualifiedName{Name: "app_t"}, GuardFormAlter, "table", false)}},
		{sql: "DROP TRIGGER IF EXISTS tr ON app_s.app_t", want: []GuardTarget{
			gt(QualifiedName{Name: "tr"}, GuardFormDrop, "trigger", false),
			gt(QualifiedName{Schema: "app_s", Name: "app_t"}, GuardFormAlter, "table", false)}},
		{sql: "DROP RULE r ON app_t CASCADE", want: []GuardTarget{
			gt(QualifiedName{Name: "r"}, GuardFormDrop, "rule", true),
			gt(QualifiedName{Name: "app_t"}, GuardFormAlter, "table", false)}},
		// A trigger literally named "on" cannot shadow the real ON.
		{sql: "DROP TRIGGER \"on\" ON app_t", want: []GuardTarget{
			gt(QualifiedName{Name: "on"}, GuardFormDrop, "trigger", false),
			gt(QualifiedName{Name: "app_t"}, GuardFormAlter, "table", false)}},
		{sql: "DROP COLLATION c", want: []GuardTarget{gt(QualifiedName{Name: "c"}, GuardFormDrop, "collation", false)}},
		// Review-6 MINOR-1: PostgreSQL accepts only the CONFIGURATION
		// spelling; the abbreviated CONFIG spelling falls out of the
		// vocabulary (and is a server syntax error anyway).
		{sql: "DROP TEXT SEARCH CONFIGURATION cfg", want: []GuardTarget{gt(QualifiedName{Name: "cfg"}, GuardFormDrop, "text-search-config", false)}},
		{sql: "ALTER TEXT SEARCH CONFIGURATION app.eng RENAME TO eng2", want: []GuardTarget{gt(QualifiedName{Schema: "app", Name: "eng"}, GuardFormAlter, "text-search-config", false)}},
		{sql: "DROP TEXT SEARCH CONFIG cfg"},
		{sql: "DROP OPERATOR CLASS oc USING btree", want: []GuardTarget{gt(QualifiedName{Name: "oc"}, GuardFormDrop, "operator-class", false)}},
		{sql: "DROP EVENT TRIGGER et", want: []GuardTarget{gt(QualifiedName{Name: "et"}, GuardFormDrop, "event-trigger", false)}},
		{sql: "DROP FOREIGN TABLE ft", want: []GuardTarget{gt(QualifiedName{Name: "ft"}, GuardFormDrop, "foreign-table", false)}},
		{sql: "DROP STATISTICS app_stat", want: []GuardTarget{gt(QualifiedName{Name: "app_stat"}, GuardFormDrop, "statistics", false)}},
		// Review-6 BLOCKER-1: operator symbol names (punctuation runs,
		// optionally schema-qualified) parse as targets.
		{sql: "DROP OPERATOR vict.===(int, int) CASCADE", want: []GuardTarget{gt(QualifiedName{Schema: "vict", Name: "==="}, GuardFormDrop, "operator", true)}},
		{sql: "DROP OPERATOR === (int4, int4)", want: []GuardTarget{gt(QualifiedName{Name: "==="}, GuardFormDrop, "operator", false)}},
		{sql: "DROP OPERATOR app.<=(text, text), >> (int, int)", want: []GuardTarget{
			gt(QualifiedName{Schema: "app", Name: "<="}, GuardFormDrop, "operator", false),
			gt(QualifiedName{Name: ">>"}, GuardFormDrop, "operator", false)}},
		{sql: "ALTER OPERATOR app.=== (int, int) OWNER TO joe", want: []GuardTarget{gt(QualifiedName{Schema: "app", Name: "==="}, GuardFormAlter, "operator", false)}},
		// Review-7 MAJOR-1: every name-class ALTER kind yields its
		// target — the extension-membership lookup runs on them.
		{sql: "ALTER OPERATOR public.<>(cube, cube) SET SCHEMA exts2", want: []GuardTarget{gt(QualifiedName{Schema: "public", Name: "<>"}, GuardFormAlter, "operator", false)}},
		{sql: "ALTER OPERATOR CLASS public.cube_ops USING btree RENAME TO cube_ops2", want: []GuardTarget{gt(QualifiedName{Schema: "public", Name: "cube_ops"}, GuardFormAlter, "operator-class", false)}},
		{sql: "ALTER OPERATOR FAMILY public.cube_ops USING btree RENAME TO cube_fam2", want: []GuardTarget{gt(QualifiedName{Schema: "public", Name: "cube_ops"}, GuardFormAlter, "operator-family", false)}},
		{sql: "ALTER COLLATION public.c RENAME TO c2", want: []GuardTarget{gt(QualifiedName{Schema: "public", Name: "c"}, GuardFormAlter, "collation", false)}},
		{sql: "ALTER STATISTICS public.st RENAME TO st2", want: []GuardTarget{gt(QualifiedName{Schema: "public", Name: "st"}, GuardFormAlter, "statistics", false)}},
		{sql: "ALTER POLICY p ON public.t RENAME TO p2", want: []GuardTarget{
			gt(QualifiedName{Name: "p"}, GuardFormAlter, "policy", false),
			gt(QualifiedName{Schema: "public", Name: "t"}, GuardFormAlter, "table", false)}},
		{sql: "ALTER TRIGGER trg ON public.t RENAME TO trg2", want: []GuardTarget{
			gt(QualifiedName{Name: "trg"}, GuardFormAlter, "trigger", false),
			gt(QualifiedName{Schema: "public", Name: "t"}, GuardFormAlter, "table", false)}},
		{sql: "ALTER RULE r ON public.t RENAME TO r2", want: []GuardTarget{
			gt(QualifiedName{Name: "r"}, GuardFormAlter, "rule", false),
			gt(QualifiedName{Schema: "public", Name: "t"}, GuardFormAlter, "table", false)}},
		{sql: "ALTER TEXT SEARCH DICTIONARY public.d RENAME TO d2", want: []GuardTarget{gt(QualifiedName{Schema: "public", Name: "d"}, GuardFormAlter, "text-search-dictionary", false)}},
		{sql: "ALTER TEXT SEARCH PARSER public.p RENAME TO p2", want: []GuardTarget{gt(QualifiedName{Schema: "public", Name: "p"}, GuardFormAlter, "text-search-parser", false)}},
		{sql: "ALTER TEXT SEARCH TEMPLATE public.t RENAME TO t2", want: []GuardTarget{gt(QualifiedName{Schema: "public", Name: "t"}, GuardFormAlter, "text-search-template", false)}},
		{sql: "DROP OWNED BY app_role", want: []GuardTarget{gt(QualifiedName{Name: "app_role"}, GuardFormDrop, "owned", false)}},
		{sql: "DROP OWNED BY a, b", want: []GuardTarget{
			gt(QualifiedName{Name: "a"}, GuardFormDrop, "owned", false),
			gt(QualifiedName{Name: "b"}, GuardFormDrop, "owned", false)}},
		{sql: "DROP TABLE innoc CASCADE", want: []GuardTarget{gt(QualifiedName{Name: "innoc"}, GuardFormDrop, "table", true)}},

		// LOW-1: non-drop ALTERs are guarded too (metadata must not be
		// modified either).
		{sql: "ALTER TABLE _neutron_probe_tbl ADD COLUMN evil text", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_probe_tbl"}, GuardFormAlter, "table", false)}},
		{sql: "ALTER SEQUENCE app_seq RESTART", want: []GuardTarget{gt(QualifiedName{Name: "app_seq"}, GuardFormAlter, "sequence", false)}},
		{sql: "ALTER MATERIALIZED VIEW app_mv OWNER TO app", want: []GuardTarget{gt(QualifiedName{Name: "app_mv"}, GuardFormAlter, "materialized-view", false)}},
		{sql: "ALTER TYPE app_t ADD VALUE 'x'", want: []GuardTarget{gt(QualifiedName{Name: "app_t"}, GuardFormAlter, "type", false)}},
		{sql: "ALTER FUNCTION app_f() RENAME TO app_g", want: []GuardTarget{gt(QualifiedName{Name: "app_f"}, GuardFormAlter, "function", false)}},
		{sql: "ALTER EXTENSION cube ADD TABLE app_t", want: []GuardTarget{gt(QualifiedName{Name: "cube"}, GuardFormAlter, "extension", false)}},

		// MINOR-2: DML writes are guarded; SELECT stays legal.
		{sql: "INSERT INTO _neutron_migrations (version) VALUES ('999')", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_migrations"}, GuardFormDML, "table", false)}},
		{sql: "UPDATE _neutron_migrations SET name = 'evil'", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_migrations"}, GuardFormDML, "table", false)}},
		{sql: "UPDATE ONLY app_t SET a = 1", want: []GuardTarget{gt(QualifiedName{Name: "app_t"}, GuardFormDML, "table", false)}},
		{sql: "DELETE FROM _neutron_migrations", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_migrations"}, GuardFormDML, "table", false)}},
		{sql: "MERGE INTO app_t USING s ON true WHEN MATCHED THEN DO NOTHING", want: []GuardTarget{gt(QualifiedName{Name: "app_t"}, GuardFormDML, "table", false)}},
		{sql: "INSERT INTO t VALUES (1)", want: []GuardTarget{gt(QualifiedName{Name: "t"}, GuardFormDML, "table", false)}},

		// Pass-2 escalation: comments cannot hide targets; data-modifying
		// CTEs and DO blocks are guarded.
		{sql: "/* note */ DROP TABLE _neutron_probe_tbl", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_probe_tbl"}, GuardFormDrop, "table", false)}},
		{sql: "-- note\nTRUNCATE _neutron_migrations", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_migrations"}, GuardFormTruncate, "table", false)}},
		{sql: "/* outer /* nested */ note */ ALTER TABLE _neutron_probe_tbl ADD COLUMN evil text", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_probe_tbl"}, GuardFormAlter, "table", false)}},
		{sql: "DROP /* between */ TABLE /* parts */ _neutron_probe_tbl", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_probe_tbl"}, GuardFormDrop, "table", false)}},
		{sql: "DROP MATERIALIZED /* split */ VIEW _neutron_probe_mv", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_probe_mv"}, GuardFormDrop, "materialized-view", false)}},
		{sql: "/* c */ WITH d AS (DELETE FROM _neutron_migrations RETURNING 1) SELECT 1", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_migrations"}, GuardFormDML, "table", false)}},
		{sql: "WITH d AS (DELETE FROM _neutron_migrations WHERE version <> '002' RETURNING 1) SELECT count(*) FROM d", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_migrations"}, GuardFormDML, "table", false)}},
		{sql: "WITH x AS (INSERT INTO app_t SELECT 1 RETURNING 1) SELECT * FROM x", want: []GuardTarget{gt(QualifiedName{Name: "app_t"}, GuardFormDML, "table", false)}},
		{sql: "WITH u AS (UPDATE app_t SET a = 1 RETURNING 1) SELECT * FROM u", want: []GuardTarget{gt(QualifiedName{Name: "app_t"}, GuardFormDML, "table", false)}},
		{sql: "WITH m AS (MERGE INTO app_t USING s ON true WHEN MATCHED THEN DO NOTHING) SELECT 1", want: []GuardTarget{gt(QualifiedName{Name: "app_t"}, GuardFormDML, "table", false)}},
		{sql: "WITH x AS (SELECT 1) INSERT INTO _neutron_migrations VALUES ('9','x')", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_migrations"}, GuardFormDML, "table", false)}},
		{sql: "WITH x AS (SELECT * FROM _neutron_migrations) SELECT count(*) FROM x"}, // read-only CTE stays legal
		{sql: "WITH total AS (SELECT count(*) AS n FROM app_t) SELECT n FROM total"},  // word update/delete never appears
		{sql: "DO $$ BEGIN CREATE TABLE do_t (i int); END $$", want: []GuardTarget{gt(QualifiedName{Name: "DO"}, GuardFormDo, "do-block", false)}},
		{sql: "/* c */ DO $$ BEGIN NULL; END $$", want: []GuardTarget{gt(QualifiedName{Name: "DO"}, GuardFormDo, "do-block", false)}},
		{sql: `SELECT $$DROP TABLE _neutron_migrations$$ AS note`}, // dollar-quoted body must not over-block
		{sql: `INSERT INTO app_t (note) VALUES ($rev$DROP TABLE _neutron_migrations$rev$)`, want: []GuardTarget{gt(QualifiedName{Name: "app_t"}, GuardFormDML, "table", false)}},

		// Review-5 BLOCKER-1: CREATE statements yield their created
		// object as a create-form target (reserved-namespace rule and
		// same-batch extension pairing live off it).
		{sql: "CREATE TABLE users (id int)", want: []GuardTarget{gt(QualifiedName{Name: "users"}, GuardFormCreate, "table", false)}},
		{sql: "CREATE TABLE IF NOT EXISTS app.logs (id int)", want: []GuardTarget{gt(QualifiedName{Schema: "app", Name: "logs"}, GuardFormCreate, "table", false)}},
		{sql: "CREATE UNLOGGED TABLE _neutron_plant (i int)", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_plant"}, GuardFormCreate, "table", false)}},
		{sql: "CREATE OR REPLACE VIEW _neutron_v AS SELECT 1", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_v"}, GuardFormCreate, "view", false)}},
		{sql: "CREATE MATERIALIZED VIEW app.mv AS SELECT 1", want: []GuardTarget{gt(QualifiedName{Schema: "app", Name: "mv"}, GuardFormCreate, "materialized-view", false)}},
		{sql: "CREATE TEMP TABLE _neutron_tmp (i int)", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_tmp"}, GuardFormCreate, "table", false)}},
		{sql: "CREATE SEQUENCE _neutron_seq", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_seq"}, GuardFormCreate, "sequence", false)}},
		{sql: "CREATE TYPE app._neutron_t AS ENUM ('a')", want: []GuardTarget{gt(QualifiedName{Schema: "app", Name: "_neutron_t"}, GuardFormCreate, "type", false)}},
		{sql: "CREATE DOMAIN _neutron_d AS int", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_d"}, GuardFormCreate, "domain", false)}},
		{sql: "CREATE SCHEMA _neutron_s", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_s"}, GuardFormCreate, "schema", false)}},
		{sql: "CREATE EXTENSION IF NOT EXISTS cube", want: []GuardTarget{gt(QualifiedName{Name: "cube"}, GuardFormCreate, "extension", false)}},
		{sql: "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS _neutron_idx ON app_t (c)", want: []GuardTarget{
			gt(QualifiedName{Name: "_neutron_idx"}, GuardFormCreate, "index", false),
			gt(QualifiedName{Name: "app_t"}, GuardFormCreate, "table", false)}},
		// Unnamed CREATE INDEX: only its ON table is a target.
		{sql: "CREATE INDEX ON _neutron_tbl (c)", want: []GuardTarget{gt(QualifiedName{Name: "_neutron_tbl"}, GuardFormCreate, "table", false)}},
		// CREATE SCHEMA AUTHORIZATION names no object.
		{sql: "CREATE SCHEMA AUTHORIZATION joe"},
		// Out-of-allowlist create kinds never reach the guard.
		{sql: "CREATE FUNCTION f() RETURNS void AS $$ SELECT 1 $$ LANGUAGE sql"},
		// Unnamed CREATE INDEX on a plain table: the table is the target.
		{sql: "CREATE INDEX ON app_t (c)", want: []GuardTarget{gt(QualifiedName{Name: "app_t"}, GuardFormCreate, "table", false)}},

		// Out of vocabulary.
		{sql: "SELECT * FROM _neutron_migrations"},
		{sql: "GRANT SELECT ON t TO public"},
	}
	for _, tc := range cases {
		got := GuardStatementTargets(tc.sql)
		if len(got) != len(tc.want) {
			t.Errorf("GuardStatementTargets(%q) = %+v, want %+v", tc.sql, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("GuardStatementTargets(%q)[%d] = %+v, want %+v", tc.sql, i, got[i], tc.want[i])
			}
		}
	}
}

func TestGuardStatementTargetsCasePreserved(t *testing.T) {
	got := GuardStatementTargets(`DROP TABLE "Users"`)
	if len(got) != 1 || got[0].Name.Name != "Users" {
		t.Fatalf("quoted case must be preserved: %+v", got)
	}
}

// TestTokenizeSQL pins the single tokenizer's contract: comments (both
// kinds, nested blocks), opaque string/dollar-quote bodies, E” escapes
// and quoted-identifier unescaping (pass-2 BLOCKER-2 design).
func TestTokenizeSQL(t *testing.T) {
	kindText := func(sql string) [][2]string {
		var out [][2]string
		for _, tok := range significantTokens(sql) {
			out = append(out, [2]string{string(tok.kind), tok.text})
		}
		return out
	}
	eq := func(sql string, want [][2]string) {
		t.Helper()
		got := kindText(sql)
		if len(got) != len(want) {
			t.Errorf("significantTokens(%q) = %v, want %v", sql, got, want)
			return
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("significantTokens(%q)[%d] = %v, want %v", sql, i, got[i], want[i])
			}
		}
	}
	eq("-- leading comment\nDROP TABLE t", [][2]string{{"w", "drop"}, {"w", "table"}, {"w", "t"}})
	eq("/* block */ TRUNCATE t", [][2]string{{"w", "truncate"}, {"w", "t"}})
	eq("/* outer /* nested */ still comment */ SELECT 1", [][2]string{{"w", "select"}, {"w", "1"}})
	eq(`DROP /* mid */ TABLE t`, [][2]string{{"w", "drop"}, {"w", "table"}, {"w", "t"}})
	// String bodies are opaque and never leak words.
	eq(`SELECT 'DROP TABLE _neutron_migrations'`, [][2]string{{"w", "select"}, {"s", `'DROP TABLE _neutron_migrations'`}})
	eq(`SELECT $$DROP TABLE _neutron_migrations$$`, [][2]string{{"w", "select"}, {"s", `$$DROP TABLE _neutron_migrations$$`}})
	eq(`SELECT $tag$; DROP TABLE _neutron_migrations$tag$`, [][2]string{{"w", "select"}, {"s", `$tag$; DROP TABLE _neutron_migrations$tag$`}})
	// E'' escapes: \' does not close the literal.
	eq(`SELECT E'foo\' DROP TABLE _neutron_migrations'`, [][2]string{{"w", "select"}, {"s", `E'foo\' DROP TABLE _neutron_migrations'`}})
	eq(`select e'a\\b'`, [][2]string{{"w", "select"}, {"s", `e'a\\b'`}})
	// '' doubling.
	eq(`SELECT 'it''s; DROP TABLE x'`, [][2]string{{"w", "select"}, {"s", `'it''s; DROP TABLE x'`}})
	// Quoted identifiers: unescaped, case preserved, may hold any char.
	eq(`DROP TABLE "Mixed ""Case"" .t"`, [][2]string{{"w", "drop"}, {"w", "table"}, {"q", `Mixed "Case" .t`}})
	// Case-insensitive keywords.
	eq("dRoP TaBlE t", [][2]string{{"w", "drop"}, {"w", "table"}, {"w", "t"}})
	// $1 parameters are punctuation + word, never names.
	eq("SELECT $1", [][2]string{{"w", "select"}, {"p", "$"}, {"w", "1"}})

	// Comment tokens are visible in the raw stream (splitter uses spans).
	all := tokenizeSQL("/* c */ -- l\nX")
	if len(all) != 3 || all[0].kind != 'c' || all[1].kind != 'c' || all[2].kind != 'w' {
		t.Errorf("raw stream must carry comment tokens: %+v", all)
	}
}

// TestHasExecutableSQLComments pins the executable filter: comment-only
// fragments — line or (nested) block — carry no executable SQL.
func TestHasExecutableSQLComments(t *testing.T) {
	yes := []string{
		"SELECT 1",
		"/* c */ SELECT 1",
		"-- c\nSELECT 1",
	}
	no := []string{
		"",
		"   \n\t ",
		"-- just a note",
		"/* just a note */",
		"/* outer /* nested */ note */",
		"-- a\n-- b\n/* c */",
	}
	for _, s := range yes {
		if !hasExecutableSQL(s) {
			t.Errorf("hasExecutableSQL(%q) = false, want true", s)
		}
	}
	for _, s := range no {
		if hasExecutableSQL(s) {
			t.Errorf("hasExecutableSQL(%q) = true, want false", s)
		}
	}
}

// TestClassifyStatementRiskComments: comments cannot hide a destructive
// statement from the acknowledgement gate (pass-2 BLOCKER-2, ack arm).
func TestClassifyStatementRiskComments(t *testing.T) {
	yes := []string{
		"/* note */ DROP TABLE t",
		"-- note\nDROP TABLE t",
		"/* /* nested */ note */ DROP TABLE t",
		"DROP /* mid */ TABLE t",
		"/* c */ ALTER TABLE t DROP COLUMN c",
	}
	no := []string{
		"-- DROP TABLE t",                // comment-only: nothing executable
		"/* DROP TABLE t */",             // comment-only: nothing executable
		"SELECT 'DROP TABLE t'",          // string body
		"SELECT $$DROP TABLE t$$",        // dollar-quoted body
		"/* c */ CREATE TABLE t (i int)", // legal under comment
	}
	for _, s := range yes {
		if d, _ := ClassifyStatementRisk(s); !d {
			t.Errorf("ClassifyStatementRisk(%q) destructive = false, want true", s)
		}
	}
	for _, s := range no {
		if d, _ := ClassifyStatementRisk(s); d {
			t.Errorf("ClassifyStatementRisk(%q) destructive = true, want false", s)
		}
	}
}

func TestGuardTargetClass(t *testing.T) {
	cases := map[string]string{
		"table": "relation", "view": "relation", "materialized-view": "relation",
		"index": "relation", "sequence": "relation", "foreign-table": "relation",
		"type": "type", "domain": "type",
		"function": "proc", "procedure": "proc", "routine": "proc", "aggregate": "proc",
		"schema": "name", "extension": "name", "policy": "name", "owned": "name",
		"operator": "name", "operator-class": "name", "operator-family": "name",
		"collation": "name", "conversion": "name", "statistics": "name",
		"trigger": "name", "rule": "name",
		"text-search-config": "name", "text-search-dictionary": "name",
		"text-search-parser": "name", "text-search-template": "name",
	}
	for kind, want := range cases {
		if got := GuardTargetClass(kind); got != want {
			t.Errorf("GuardTargetClass(%q) = %q, want %q", kind, got, want)
		}
	}
}

// TestCascadeDispatchCoversEveryAllowlistedDropKind is the dispatch half of
// review-6 BLOCKER-1's permanent regression surface: every kind the drop
// allowlist accepts must reach the shared pg_depend closure through exactly
// one arm — the extension member rule, the schema namespace-containment
// seed, relation-class resolution, or a per-class seed catalog entry. A new
// allowlisted kind without a seed fails here (and the E2E property table
// fails on its missing fixture).
func TestCascadeDispatchCoversEveryAllowlistedDropKind(t *testing.T) {
	for _, kind := range AllowlistedDropKinds() {
		switch {
		case kind == "extension", kind == "schema":
			// Special arms: member rule and namespace seed.
		case GuardTargetClass(kind) == "relation":
			// Resolved through pg_class (to_regclass).
		default:
			if _, ok := cascadeSeedCatalogs[kind]; !ok {
				t.Errorf("drop kind %q has no cascade seed arm — a CASCADE of this kind would bypass the closure", kind)
			}
		}
	}
	// The catalog map must not carry dead entries for kinds outside the
	// allowlist (they would be unreachable dispatch).
	for kind := range cascadeSeedCatalogs {
		if !dropAllowKinds[kind] {
			t.Errorf("cascade seed catalog carries non-allowlisted kind %q", kind)
		}
	}
}

// TestNameClassMembershipCoversEveryAllowlistedAlterKind is review-7
// MAJOR-1's permanent regression surface on the unit side: every
// allowlisted ALTER kind of the "name" guard target class must be a
// cascadeSeedCatalogs key, so the name-class extension-membership lookup
// has a catalog for it — a new name-class alter kind without a seed
// fails here (and the E2E property table fails on its missing fixture).
// Relation/type/proc kinds are covered by the original class-aware arms
// and stay outside this table.
func TestNameClassMembershipCoversEveryAllowlistedAlterKind(t *testing.T) {
	seen := 0
	for _, kind := range AllowlistedAlterKinds() {
		if kind == "extension" {
			// Dedicated arm: every ALTER EXTENSION is refused
			// wholesale before any membership question.
			continue
		}
		if GuardTargetClass(kind) != "name" {
			continue
		}
		seen++
		if _, ok := cascadeSeedCatalogs[kind]; !ok {
			t.Errorf("name-class alter kind %q has no membership seed catalog — an ALTER of an extension member of this kind would bypass the membership arm", kind)
		}
	}
	if seen == 0 {
		t.Fatal("no name-class allowlisted alter kinds enumerated — the property table went vacuous")
	}
}

func TestStatementPostconditionOf(t *testing.T) {
	cases := []struct {
		sql    string
		kind   string
		name   string
		schema string
		ok     bool
	}{
		{sql: "CREATE TABLE users (id int)", kind: PostconditionRelationCreate, name: "users", ok: true},
		{sql: "create table if not exists app.logs (id int)", kind: PostconditionRelationCreate, name: "logs", schema: "app", ok: true},
		{sql: `CREATE TABLE "Mixed Case" (id int)`, kind: PostconditionRelationCreate, name: "Mixed Case", ok: true},
		{sql: "CREATE OR REPLACE VIEW v AS SELECT 1", kind: PostconditionRelationCreate, name: "v", ok: true},
		{sql: "CREATE MATERIALIZED VIEW mv AS SELECT 1", kind: PostconditionRelationCreate, name: "mv", ok: true},
		{sql: "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx ON t (c)", kind: PostconditionIndexCreate, name: "idx", ok: true},
		{sql: "CREATE INDEX idx ON app.t (c)", kind: PostconditionIndexCreate, name: "idx", schema: "app", ok: true},
		{sql: "CREATE INDEX ON t (c)", ok: false},
		{sql: "CREATE TYPE app.status AS ENUM ('a')", kind: PostconditionTypeCreate, name: "status", schema: "app", ok: true},
		{sql: "CREATE SCHEMA IF NOT EXISTS side", kind: PostconditionSchemaCreate, name: "side", ok: true},
		{sql: "DROP TABLE IF EXISTS users", kind: PostconditionRelationDrop, name: "users", ok: true},
		{sql: "DROP INDEX CONCURRENTLY idx", kind: PostconditionIndexDrop, name: "idx", ok: true},
		{sql: "DROP TYPE IF EXISTS app.status", kind: PostconditionTypeDrop, name: "status", schema: "app", ok: true},
		{sql: "DROP SCHEMA IF EXISTS s CASCADE", kind: PostconditionSchemaDrop, name: "s", ok: true},
		{sql: "ALTER TABLE t ADD COLUMN c int", ok: false},
		{sql: "INSERT INTO t VALUES (1)", ok: false},
		{sql: "SELECT pg_sleep(3)", ok: false},
		{sql: "CREATE FUNCTION f() RETURNS void AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql", ok: false},
	}
	for _, tc := range cases {
		post, ok := StatementPostconditionOf(tc.sql)
		if ok != tc.ok {
			t.Errorf("StatementPostconditionOf(%q) ok = %v, want %v", tc.sql, ok, tc.ok)
			continue
		}
		if !ok {
			continue
		}
		if post.Kind != tc.kind || post.Name != tc.name || post.Schema != tc.schema {
			t.Errorf("StatementPostconditionOf(%q) = %+v, want kind=%s name=%s schema=%s", tc.sql, post, tc.kind, tc.name, tc.schema)
		}
	}
}

func TestPlanMatchesUpSQL(t *testing.T) {
	ops := DiffResult{
		Up: []string{
			`create table "public"."users" ("id" integer not null)`,
			`alter table "public"."users" add constraint "users_pk" primary key ("id")`,
		},
		Down: []string{
			`drop table "public"."users"`,
			`alter table "public"."users" drop constraint "users_pk"`,
		},
	}
	doc, err := EmptyV2Document()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlanArtifact("003", "add_users", "001_base", "deadbeef", doc, nil, ops)
	if err != nil {
		t.Fatal(err)
	}
	// Render exactly like MigrationArtifactSet does.
	upSQL := "-- Migration: add_users\n\n" + strings.Join(ops.Up, ";\n") + ";\n"
	if err := PlanMatchesUpSQL(plan, upSQL); err != nil {
		t.Fatalf("generated up.sql must match its plan: %v", err)
	}

	// Hand-edited up.sql (statement appended): stale.
	tampered := upSQL + "\nCREATE TABLE extra (id int);\n"
	if err := PlanMatchesUpSQL(plan, tampered); err == nil {
		t.Fatal("appended statement accepted despite stale plan")
	} else if !strings.Contains(err.Error(), "stale plan") {
		t.Errorf("mismatch must be named stale: %v", err)
	}

	// Hand-edited up.sql (statement changed in place): stale.
	changed := strings.Replace(upSQL, `"id" integer`, `"id" bigint`, 1)
	if err := PlanMatchesUpSQL(plan, changed); err == nil {
		t.Fatal("modified statement accepted despite stale plan")
	}

	// Comment-only additions do not count as statements.
	if err := PlanMatchesUpSQL(plan, upSQL+"\n-- a note\n"); err != nil {
		t.Fatalf("comment-only addition must not be staleness: %v", err)
	}
}

func TestLoadPlanArtifact(t *testing.T) {
	ops := DiffResult{Up: []string{"CREATE TABLE a (id int)"}, Down: []string{"DROP TABLE a"}}
	doc, err := EmptyV2Document()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlanArtifact("001", "one", "empty", doc.SHA256Hex, doc, nil, ops)
	if err != nil {
		t.Fatal(err)
	}
	content, err := MarshalPlanJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "001_one.plan.json")
	if err := writeTestFile(path, content); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadPlanArtifact(path)
	if err != nil {
		t.Fatalf("LoadPlanArtifact: %v", err)
	}
	if err := VerifyPlanIdentity(loaded, "001", "one"); err != nil {
		t.Errorf("VerifyPlanIdentity: %v", err)
	}
	if err := VerifyPlanIdentity(loaded, "002", "one"); err == nil {
		t.Error("identity mismatch accepted")
	}

	// Unknown fields are refused.
	withUnknown := strings.Replace(string(content), `"formatVersion": 1`, `"formatVersion": 1, "bogus": true`, 1)
	if err := writeTestFile(path, []byte(withUnknown)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPlanArtifact(path); err == nil {
		t.Error("unknown field accepted")
	}

	// Foreign workflow tag refused.
	foreign := strings.Replace(string(content), `"workflow": "snapshot-v1"`, `"workflow": "other-v9"`, 1)
	if err := writeTestFile(path, []byte(foreign)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPlanArtifact(path); err == nil {
		t.Error("foreign workflow accepted")
	}

	// LOW-2 (M05 review): trailing garbage after the JSON value is a
	// malformed artifact, not a readable plan.
	if err := writeTestFile(path, append(append([]byte{}, content...), []byte("garbage")...)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPlanArtifact(path); err == nil {
		t.Error("trailing garbage accepted")
	} else if !strings.Contains(err.Error(), "trailing content") {
		t.Errorf("trailing garbage must be named: %v", err)
	}
	if err := writeTestFile(path, append(append([]byte{}, content...), []byte("{}")...)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPlanArtifact(path); err == nil {
		t.Error("trailing second JSON value accepted")
	}
	// Trailing whitespace alone stays fine.
	if err := writeTestFile(path, append(append([]byte{}, content...), []byte("\n  \n")...)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPlanArtifact(path); err != nil {
		t.Errorf("trailing whitespace must not be refused: %v", err)
	}
}

func TestStatementKind(t *testing.T) {
	allowed := []struct{ sql, kind string }{
		{"SELECT 1", "select"},
		{"select count(*) from _neutron_migrations", "select"},
		{"SELECT $$DROP TABLE x$$", "select"},
		{"WITH x AS (SELECT 1) SELECT * FROM x", "with"},
		{"WITH d AS (DELETE FROM t RETURNING 1) SELECT count(*) FROM d", "with"},
		{"INSERT INTO t VALUES (1)", "insert"},
		{"update t set a = 1", "update"},
		{"UPDATE ONLY t SET a = 1", "update"},
		{"DELETE FROM t", "delete"},
		{"MERGE INTO t USING s ON true WHEN MATCHED THEN DO NOTHING", "merge"},
		{"TRUNCATE t", "truncate"},
		{"TRUNCATE TABLE a, b", "truncate"},
		{"CREATE TABLE a (id int)", "create table"},
		{"CREATE UNLOGGED TABLE a (id int)", "create table"},
		{"CREATE TEMP TABLE a (id int)", "create table"},
		{"CREATE TABLE IF NOT EXISTS a (id int)", "create table"},
		{"CREATE INDEX i ON t (c)", "create index"},
		{"CREATE UNIQUE INDEX i ON t (c)", "create index"},
		{"CREATE INDEX CONCURRENTLY i ON t (c)", "create index"},
		{"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS i ON t (c)", "create index"},
		{"CREATE SCHEMA s", "create schema"},
		{"CREATE SCHEMA IF NOT EXISTS s", "create schema"},
		{"CREATE TYPE t AS ENUM ('a')", "create type"},
		{"CREATE DOMAIN d AS int", "create domain"},
		{"CREATE VIEW v AS SELECT 1", "create view"},
		{"CREATE OR REPLACE VIEW v AS SELECT 1", "create view"},
		{"CREATE MATERIALIZED VIEW mv AS SELECT 1", "create materialized view"},
		{"CREATE SEQUENCE s", "create sequence"},
		{"CREATE EXTENSION cube", "create extension"},
		{"CREATE EXTENSION IF NOT EXISTS plpgsql", "create extension"},
		{"ALTER TABLE a ADD COLUMN b int", "alter table"},
		{"ALTER INDEX i RENAME TO j", "alter index"},
		{"ALTER SEQUENCE s RESTART", "alter sequence"},
		{"ALTER TYPE t ADD VALUE 'c'", "alter type"},
		{"ALTER VIEW v OWNER TO joe", "alter view"},
		{"DROP TABLE a", "drop table"},
		{"DROP INDEX CONCURRENTLY i", "drop index"},
		{"DROP MATERIALIZED VIEW mv", "drop materialized-view"},
		{"DROP SEQUENCE s", "drop sequence"},
		{"DROP TYPE t", "drop type"},
		{"DROP DOMAIN d", "drop domain"},
		{"DROP FUNCTION f(int)", "drop function"},
		{"DROP SCHEMA s CASCADE", "drop schema"},
		{"DROP EXTENSION cube", "drop extension"},
		// Review-6 MINOR-1: the CONFIGURATION spelling (PG's only legal
		// one) is live in the drop/alter allowlists.
		{"DROP TEXT SEARCH CONFIGURATION c", "drop text-search-config"},
		{"ALTER TEXT SEARCH CONFIGURATION c RENAME TO d", "alter text-search-config"},
		{"DROP OPERATOR CLASS oc USING btree", "drop operator-class"},
		{"DROP OPERATOR FAMILY of USING btree", "drop operator-family"},
		{"DROP TEXT SEARCH DICTIONARY d", "drop text-search-dictionary"},
		{"DROP STATISTICS st", "drop statistics"},
		{"DROP CONVERSION cv", "drop conversion"},
		// Review-7 MAJOR-1: every name-class ALTER kind is live in the
		// allowlist (the membership guard runs on their targets).
		{"ALTER OPERATOR public.<>(cube, cube) SET SCHEMA s", "alter operator"},
		{"ALTER OPERATOR CLASS oc USING btree RENAME TO oc2", "alter operator-class"},
		{"ALTER OPERATOR FAMILY of USING btree RENAME TO of2", "alter operator-family"},
		{"ALTER COLLATION public.c RENAME TO c2", "alter collation"},
		{"ALTER STATISTICS public.st RENAME TO st2", "alter statistics"},
		{"ALTER POLICY p ON public.t RENAME TO p2", "alter policy"},
		{"ALTER TRIGGER trg ON public.t RENAME TO trg2", "alter trigger"},
		{"ALTER RULE r ON public.t RENAME TO r2", "alter rule"},
		{"ALTER TEXT SEARCH DICTIONARY public.d RENAME TO d2", "alter text-search-dictionary"},
		{"ALTER TEXT SEARCH PARSER public.p RENAME TO p2", "alter text-search-parser"},
		{"ALTER TEXT SEARCH TEMPLATE public.t RENAME TO t2", "alter text-search-template"},
		{"SET LOCAL statement_timeout = '10s'", "set local"},
		{"set local lock_timeout = '5s'", "set local"},
		// Comments cannot shift a kind in either direction.
		{"/* c */ CREATE TABLE a (id int)", "create table"},
		{"CREATE /* split */ TABLE a (id int)", "create table"},
		{"-- header\nSELECT 1", "select"},
	}
	for _, tc := range allowed {
		if got := StatementKind(tc.sql); got != tc.kind {
			t.Errorf("StatementKind(%q) = %q, want %q", tc.sql, got, tc.kind)
		}
	}

	refused := []string{
		"EXPLAIN SELECT 1",
		"EXPLAIN ANALYZE DELETE FROM _neutron_migrations",
		"PREPARE p AS DELETE FROM _neutron_migrations",
		"EXECUTE p",
		"DEALLOCATE p",
		"DO $$ BEGIN NULL; END $$",
		"CALL p()",
		"COPY t FROM STDIN",
		"COPY t FROM PROGRAM 'echo x'",
		"CREATE FUNCTION f() RETURNS void AS $$ SELECT 1 $$ LANGUAGE sql",
		"CREATE PROCEDURE p() LANGUAGE plpgsql AS $$ NULL; $$",
		"CREATE RULE r AS ON DELETE TO t DO INSTEAD DELETE FROM u",
		"CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
		"CREATE POLICY p ON t USING (true)",
		"CREATE AGGREGATE a (int) (SFUNC = f, STYPE = int)",
		"CREATE COLLATION c (locale = 'en_US')",
		"CREATE DATABASE db",
		"CREATE ROLE r",
		"CREATE EVENT TRIGGER trg ON ddl_command_start EXECUTE FUNCTION f()",
		"CREATE TEXT SEARCH CONFIGURATION c (PARSER = p)",
		"CREATE OPERATOR CLASS oc FOR TYPE int USING btree AS FAMILY opf",
		"CREATE PUBLICATION pub FOR TABLE t",
		"CREATE SERVER s FOREIGN DATA WRAPPER fdw",
		"CREATE STATISTICS st ON a, b FROM t",
		"GRANT SELECT ON t TO public",
		"REVOKE SELECT ON t FROM public",
		"COMMENT ON TABLE t IS 'note'",
		"VACUUM t",
		"ANALYZE t",
		"REINDEX TABLE t",
		"REINDEX CONCURRENTLY INDEX i",
		"LOCK TABLE t",
		"SAVEPOINT sp",
		"BEGIN",
		"COMMIT",
		"START TRANSACTION",
		"SET statement_timeout = '10s'",
		"SET standard_conforming_strings = off",
		"LISTEN ch",
		"NOTIFY ch",
		"SHOW ALL",
		"RESET ALL",
		"DISCARD ALL",
		"CLOSE cur",
		"DECLARE cur CURSOR FOR SELECT 1",
		"FETCH 1 FROM cur",
		"SECURITY LABEL ON TABLE t IS 'x'",
		"IMPORT FOREIGN SCHEMA s FROM SERVER srv LIMIT TO (t)",
		"/* c */ EXPLAIN SELECT 1",
		"-- header\nCOPY t FROM STDIN",
		"CLUSTER t USING i",
		"CHECKPOINT",
		// Review-4 BLOCKER-1: database-wide and role-wide kinds are
		// refused by the allowlist even though the guard vocabulary
		// still classifies them (protected-name detection stays wide).
		"DROP DATABASE other_db",
		"DROP DATABASE IF EXISTS other_db",
		"ALTER DATABASE other_db SET search_path = public",
		"DROP TABLESPACE ts",
		"ALTER TABLESPACE ts OWNER TO joe",
		"DROP ROLE r",
		"DROP USER u",
		"DROP GROUP g",
		"ALTER ROLE r SET statement_timeout = '10s'",
		"ALTER ROLE r WITH LOGIN",
		"ALTER USER u SET work_mem = '64MB'",
		"ALTER GROUP g ADD USER u",
		"DROP OWNED BY joe",
		"DROP OWNED BY a, b",
		"DROP PUBLICATION pub",
		"DROP SUBSCRIPTION sub",
		"ALTER PUBLICATION pub ADD TABLE t",
		"ALTER SUBSCRIPTION sub CONNECTION 'conninfo'",
		"DROP SERVER s",
		"DROP LANGUAGE l",
		"DROP CAST (int AS text)",
		"DROP TRANSFORM FOR int LANGUAGE sql",
		"DROP EVENT TRIGGER et",
		"DROP FOREIGN DATA WRAPPER fdw",
		"DROP ACCESS METHOD am",
		"DROP USER MAPPING FOR u SERVER s",
		// Comment-shift cannot turn a refused kind allowed either.
		"/* c */ DROP DATABASE other_db",
		"-- header\nALTER ROLE r SET statement_timeout = '10s'",
	}
	for _, sql := range refused {
		if got := StatementKind(sql); got != "" {
			t.Errorf("StatementKind(%q) = %q, want refused (\"\")", sql, got)
		}
	}

	// Comment-only fragments have no kind (callers skip them).
	if got := StatementKind("-- nothing here"); got != "" {
		t.Errorf("comment-only statement classified as %q", got)
	}
	if got := StatementKind("/* block */"); got != "" {
		t.Errorf("block-comment-only statement classified as %q", got)
	}
}

func TestCheckStatementAllowlist(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1",
		"CREATE TABLE a (id int)",
		"DROP TABLE a",
		"SET LOCAL statement_timeout = '10s'",
		"-- comment only\n",
		"/* block only */",
		"",
	} {
		if err := CheckStatementAllowlist(sql); err != nil {
			t.Errorf("CheckStatementAllowlist(%q) = %v, want nil", sql, err)
		}
	}

	cases := []struct {
		sql    string
		label  string
		reason string
	}{
		{"EXPLAIN ANALYZE DELETE FROM _neutron_migrations", "EXPLAIN", "executes the statements it plans"},
		{"PREPARE p AS DELETE FROM _neutron_migrations", "PREPARE", "EXECUTE time"},
		{"EXECUTE p", "EXECUTE", "not visible to validation"},
		{"DO $$ BEGIN NULL; END $$", "DO", "opaque"},
		{"CALL p()", "CALL", "opaque"},
		{"COPY t FROM STDIN", "COPY", "stalls the batch"},
		{"COPY t FROM PROGRAM 'echo x'", "COPY", "arbitrary program execution"},
		{"CREATE FUNCTION f() RETURNS void AS $$ SELECT 1 $$ LANGUAGE sql", "CREATE FUNCTION", "opaque"},
		{"CREATE PROCEDURE p() LANGUAGE plpgsql AS $$ NULL; $$", "CREATE PROCEDURE", "opaque"},
		{"CREATE RULE r AS ON DELETE TO t DO INSTEAD DELETE FROM u", "CREATE RULE", "execution time"},
		// Review-4 MINOR-1: OR REPLACE spellings keep the precise
		// per-kind label and reason (no degraded "CREATE OR").
		{"CREATE OR REPLACE FUNCTION f() RETURNS void LANGUAGE sql AS $$ SELECT 1 $$", "CREATE FUNCTION", "opaque"},
		{"CREATE OR REPLACE PROCEDURE p() LANGUAGE plpgsql AS $$ NULL; $$", "CREATE PROCEDURE", "opaque"},
		{"CREATE OR REPLACE RULE r AS ON DELETE TO t DO INSTEAD DELETE FROM u", "CREATE RULE", "execution time"},
		{"CREATE OR REPLACE TRIGGER trg AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()", "CREATE TRIGGER", "opaque execution route"},
		// Review-4 BLOCKER-1: database-wide and role-wide kinds.
		{"DROP DATABASE other_db", "DROP DATABASE", "schema objects only"},
		{"ALTER DATABASE other_db SET search_path = public", "ALTER DATABASE", "schema objects only"},
		{"CREATE DATABASE other_db", "CREATE DATABASE", "schema objects only"},
		{"DROP TABLESPACE ts", "DROP TABLESPACE", "schema objects only"},
		{"DROP ROLE r", "DROP ROLE", "schema objects only"},
		{"ALTER ROLE r SET statement_timeout = '10s'", "ALTER ROLE", "schema objects only"},
		// Review-5 MINOR-2: multi-word refused kinds label canonically,
		// not as a truncated first word (and USER MAPPING no longer
		// mis-attributes the role-wide reason).
		{"DROP FOREIGN DATA WRAPPER fdw", "DROP FOREIGN DATA WRAPPER", "not in the migration allowlist"},
		{"CREATE EVENT TRIGGER trg ON ddl_command_start EXECUTE FUNCTION f()", "CREATE EVENT TRIGGER", "not in the migration allowlist"},
		{"CREATE ACCESS METHOD am TYPE btree HANDLER h", "CREATE ACCESS METHOD", "not in the migration allowlist"},
		{"ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO public", "ALTER DEFAULT PRIVILEGES", "not in the migration allowlist"},
		{"CREATE USER MAPPING FOR joe SERVER s", "CREATE USER MAPPING", "not in the migration allowlist"},
		// Review-6 MINOR-1: the abbreviated CONFIG spelling (a server
		// syntax error in PostgreSQL) stays refused rather than
		// half-allowlisted.
		{"DROP TEXT SEARCH CONFIG c", "DROP TEXT", "not in the migration allowlist"},
		{"GRANT SELECT ON t TO public", "GRANT", "outside this alpha"},
		{"COMMENT ON TABLE t IS 'x'", "COMMENT", "outside this alpha"},
		{"VACUUM t", "VACUUM", "maintenance"},
		{"SET statement_timeout = '10s'", "SET", "SET LOCAL"},
		{"BEGIN", "BEGIN", "runner owns transactions"},
		{"LOCK TABLE t", "LOCK", "owns locking"},
		{"SAVEPOINT sp", "SAVEPOINT", "owns transaction structure"},
	}
	for _, tc := range cases {
		err := CheckStatementAllowlist(tc.sql)
		if err == nil {
			t.Errorf("CheckStatementAllowlist(%q) accepted an out-of-allowlist statement", tc.sql)
			continue
		}
		msg := err.Error()
		if !strings.Contains(msg, "statement kind "+tc.label+" is refused") {
			t.Errorf("refusal for %q must name the kind %q: %s", tc.sql, tc.label, msg)
		}
		if !strings.Contains(msg, tc.reason) {
			t.Errorf("refusal for %q must carry the reason %q: %s", tc.sql, tc.reason, msg)
		}
		if !strings.Contains(msg, "migrations may contain only") {
			t.Errorf("refusal must state the allowlist model: %s", msg)
		}
	}
}
