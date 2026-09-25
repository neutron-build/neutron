package inspect

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestCheckReadOnlySQLBothEngines(t *testing.T) {
	allowed := []string{
		"SELECT 1",
		"select * from users where name = 'DELETE me; now' -- update\n",
		"WITH t AS (SELECT 1) SELECT * FROM t;",
		"SHOW search_path",
		"EXPLAIN SELECT * FROM orders",
		"VALUES (1), (2)",
		"TABLE users",
		`SELECT "delete" FROM t`,
		"SELECT $body$ insert into x $body$",
		"SELECT E'it\\'s ; fine'",
		"SELECT /* drop table x; */ 1",
		"SELECT * FROM t WHERE id = $1",
	}
	for _, sql := range allowed {
		for _, product := range []string{"postgres", "nucleus"} {
			if err := CheckReadOnlySQL(sql, product); err != nil {
				t.Errorf("%s: %q refused: %v", product, sql, err)
			}
		}
	}
	refusedEverywhere := []string{
		"INSERT INTO t VALUES (1)",
		"DELETE FROM t",
		"SELECT 1; DROP TABLE t",
		"SELECT 1; ; SELECT 2",
		"COMMIT",
		"SET TRANSACTION READ WRITE",
		"SELECT pg_advisory_lock(1)",
		"SELECT pg_catalog.pg_terminate_backend(123)",
		"SELECT \"pg_cancel_backend\"(1)",
		"select dblink_exec('x', 'drop table t')",
		"SELECT 'unterminated",
		"SELECT /* unterminated",
		"",
		"   ;  ",
	}
	for _, sql := range refusedEverywhere {
		for _, product := range []string{"postgres", "nucleus"} {
			err := CheckReadOnlySQL(sql, product)
			var ge *GuardError
			if !errors.As(err, &ge) {
				t.Errorf("%s: %q allowed (err=%v)", product, sql, err)
			}
		}
	}
}

// On Nucleus the guard is the only enforcement: everything PostgreSQL's
// READ ONLY transaction would refuse must be refused lexically.
func TestCheckReadOnlySQLNucleusStrict(t *testing.T) {
	cases := []string{
		"WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
		"SELECT * INTO copy FROM t",
		"SELECT KV_SET('k', 'v')",
		"select pg_catalog.kv_set('k','v')",
		"SELECT DOC_INSERT('c', '{}')",
		"SELECT GRAPH_ADD_NODE('X')",
		"SELECT nextval('s')",
		"SELECT set_config('app.tenant', '2', false)",
		"SELECT * FROM t FOR UPDATE",
		"SELECT * FROM t FOR NO KEY UPDATE",
		"EXPLAIN ANALYZE SELECT 1",
		"EXPLAIN (ANALYZE, BUFFERS) SELECT 1",
		"SELECT * FROM t WHERE ts_insert('s', 1, 2) IS NOT NULL",
		"SELECT STREAM_XREADGROUP('s','g','c',1)",
	}
	for _, sql := range cases {
		if err := CheckReadOnlySQL(sql, "nucleus"); err == nil {
			t.Errorf("nucleus: %q allowed", sql)
		}
	}
	// The same statements are left to PostgreSQL's READ ONLY transaction
	// (engine enforcement), except the functions it cannot undo.
	for _, sql := range []string{
		"WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
		"SELECT * INTO copy FROM t",
		"EXPLAIN ANALYZE SELECT 1",
	} {
		if err := CheckReadOnlySQL(sql, "postgres"); err != nil {
			t.Errorf("postgres: %q refused lexically (the engine enforces it): %v", sql, err)
		}
	}
	if err := CheckReadOnlySQL("SELECT 1", "unknown"); err != nil {
		t.Errorf("unknown engine refuses a plain read: %v", err)
	}
	if err := CheckReadOnlySQL("SELECT kv_set('a','b')", "unknown"); err == nil {
		t.Errorf("unknown engine: strict guard not applied")
	}
}

func TestCheckReadOnlyCypher(t *testing.T) {
	for _, q := range []string{
		"MATCH (n) RETURN n LIMIT 25",
		"MATCH (n:Order) WHERE n.sqlref_table = 'orders' RETURN n",
		"MATCH (n) WHERE n.create = 1 RETURN n",
		"MATCH (n) WHERE n.name = 'DELETE ME' RETURN n",
		"MATCH (n:Set) RETURN n",
	} {
		if err := CheckReadOnlyCypher(q); err != nil {
			t.Errorf("%q refused: %v", q, err)
		}
	}
	for _, q := range []string{
		"CREATE (n:X)",
		"MATCH (n) DETACH DELETE n",
		"MATCH (n) SET n.x = 1",
		"MERGE (n:X {id: 1})",
		"MATCH (n) REMOVE n.x",
		"match (n) delete n",
		"MATCH (n) RETURN n /* unterminated",
		"MATCH (n) WHERE n.x = 'open",
	} {
		if err := CheckReadOnlyCypher(q); err == nil {
			t.Errorf("%q allowed", q)
		}
	}
}

// The Nucleus denylist is the engine's own classification: every name the
// measured build lists as mutating or side-effecting must be refused.
func TestNucleusMutatingListTracksEngine(t *testing.T) {
	quoted := regexp.MustCompile(`"([A-Z][A-Z0-9_]+)"`)
	extract := func(file, start string) []string {
		raw, err := os.ReadFile(filepath.Join(repoRoot, file))
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		src := string(raw)
		i := strings.Index(src, start)
		if i < 0 {
			t.Fatalf("%s: %q not found", file, start)
		}
		end := strings.Index(src[i:], "];")
		if end < 0 {
			t.Fatalf("%s: end of %q not found", file, start)
		}
		var names []string
		for _, m := range quoted.FindAllStringSubmatch(src[i:i+end], -1) {
			names = append(names, m[1])
		}
		if len(names) == 0 {
			t.Fatalf("%s: no names under %q", file, start)
		}
		return names
	}
	var all []string
	all = append(all, extract("nucleus/src/executor/scalar_fns.rs", "pub(crate) const SIDE_EFFECTING_FN_NAMES")...)
	all = append(all, extract("nucleus/src/executor/admission.rs", "pub(super) const MUTATING_SCALAR_FNS:")...)
	all = append(all, extract("nucleus/src/executor/admission.rs", "pub(super) const MUTATING_SCALAR_FNS_EXTRA")...)
	for _, name := range all {
		if !NucleusMutatingFns[name] {
			t.Errorf("engine classifies %s as mutating; the guard does not refuse it", name)
		}
		if err := CheckReadOnlySQL("SELECT "+strings.ToLower(name)+"('x')", "nucleus"); err == nil {
			t.Errorf("SELECT %s(...) allowed on nucleus", name)
		}
	}
}

// Review 1 (HIGH-1/HIGH-2) bypasses and the forms found while fixing them.
// Every one of these ran on the pre-fix guard; see live_test.go and
// live_nucleus_test.go for the same statements against real engines.
func TestGuardBypassRegressions(t *testing.T) {
	both := []string{
		// Vertical tab: whitespace to PostgreSQL and sqlparser, not to the old scanner.
		"SELECT pg_advisory_lock\v(4343)",
		"SELECT pg_catalog.pg_advisory_lock\v(4646), pg_terminate_backend\v(-1)",
		// U& identifiers, default escape, 6-digit escape and UESCAPE.
		`SELECT U&"pg_\0061dvisory_lock"(4545)`,
		`SELECT u&"pg_\+000061dvisory_lock"(4545)`,
		`SELECT U&"pg_!0061dvisory_lock" UESCAPE '!' (4545)`,
		`SELECT U&"pg_!0061dvisory_lock" /* c */ UESCAPE '!' (4545)`,
		// Anything between name and '(' (comments, all whitespace), and no '(' at all.
		"SELECT pg_advisory_lock/**/(1)",
		"SELECT pg_advisory_lock /* a /* nested */ b */ (1)",
		"SELECT pg_advisory_lock -- c\n(1)",
		"SELECT pg_advisory_lock\f\t\r\n (1)",
		"SELECT pg_advisory_lock",
		// Case and quoting.
		"SELECT Pg_Advisory_Lock(1)",
		`SELECT "pg_advisory_lock"(1)`,
		// A line comment ends at \r on both servers.
		"SELECT 1 --x\r, pg_advisory_lock(5050)",
		// standard_conforming_strings = off: the literal ends at the second quote.
		`SELECT '\'', pg_advisory_lock(5151) --'`,
		// Non-ASCII whitespace separates tokens here (the servers read it as
		// part of the name, so this only ever refuses more).
		"SELECT pg_advisory_lock\u00a0(1)",
		// Effects that escape a rollback and the session.
		"SELECT pg_stat_reset()",
		"SELECT pg_stat_reset_shared('bgwriter')",
		"SELECT pg_stat_reset_single_table_counters(1)",
		"SELECT pg_stat_statements_reset()",
		"SELECT pg_logical_emit_message(false, 'x06', 'escaped')",
		"SELECT pg_replication_slot_advance('s', '0/0')",
		"SELECT pg_copy_logical_replication_slot('a', 'b')",
		"SELECT pg_replication_origin_create('o')",
		"SELECT * FROM pg_logical_slot_get_changes('s', NULL, NULL)",
		"SELECT dblink_connect('c', 'dbname=x')",
		"SELECT * FROM dblink('c', 'select 1') AS t(a int)",
		// Refused, not guessed.
		"SELECT 1\x01",
		`SELECT U&"\zz"(1)`,
		`SELECT U&"x" UESCAPE 'ab'`,
		"SELECT $é$ x $é$",
	}
	for _, sql := range both {
		for _, product := range []string{"postgres", "nucleus"} {
			var ge *GuardError
			if err := CheckReadOnlySQL(sql, product); !errors.As(err, &ge) {
				t.Errorf("%s: %q allowed (err=%v)", product, sql, err)
			}
		}
	}
	nucleus := []string{
		"SELECT NEXTVAL\v('nseq')",
		"SELECT NeXtVaL /* x */ ('nseq')",
		"SELECT KV_SET\v('k', 'v')",
		"SELECT DATALOG_ASSERT\v('p(a)')",
		"SELECT FTS_INDEX\v(1, 'x')",
		"SELECT BLOB_STORE\v('b', 'x')",
		"SELECT TS_INSERT\v('s', 1, 2)",
		"SELECT KV_LPUSH\v('k', 'v')",
		"SELECT COLUMNAR_INSERT\v('t', 'a', 1)",
		"SELECT PUBSUB_PUBLISH\v('c', 'm')",
		`SELECT U&"kv_\0073et"('k', 'v')`,
		// HIGH-2: GRAPH_QUERY executes write Cypher.
		"SELECT GRAPH_QUERY('CREATE (n:X06REV {a: 1})')",
		"SELECT graph_query\v('MATCH (n) DETACH DELETE n')",
		"SELECT GRAPH_QUERY($$CREATE (n)$$)",
		"SELECT GRAPH_QUERY('MATCH (n) RETURN n' || ' CREATE (m)')",
		"SELECT GRAPH_QUERY(E'CREATE (n)')",
		"SELECT GRAPH_QUERY(U&'CREATE (n)')",
		"SELECT GRAPH_QUERY(q) FROM t",
		"SELECT GRAPH_QUERY",
		`SELECT "graph_query"('CREATE (n)')`,
	}
	for _, sql := range nucleus {
		if err := CheckReadOnlySQL(sql, "nucleus"); err == nil {
			t.Errorf("nucleus: %q allowed", sql)
		}
	}
	allowed := []string{
		"SELECT\v1",
		`SELECT 'a\b'`,
		`SELECT 'C:\'`,
		`SELECT E'\x41', E'it\'s'`,
		`SELECT U&"d\0061ta" FROM t`,
		`SELECT U&'\0041'`,
		"SELECT $tag$ pg_advisory_lock(1) $tag$",
		"SELECT 'pg_advisory_lock(1)'",
		"SELECT 1 -- pg_advisory_lock(1)",
	}
	for _, sql := range allowed {
		for _, product := range []string{"postgres", "nucleus"} {
			if err := CheckReadOnlySQL(sql, product); err != nil {
				t.Errorf("%s: %q refused: %v", product, sql, err)
			}
		}
	}
	for _, sql := range []string{
		"SELECT GRAPH_QUERY('MATCH (n) RETURN n LIMIT 5')",
		"SELECT GRAPH_QUERY($$MATCH (n) RETURN count(n)$$)",
		"SELECT pg_catalog.graph_query('MATCH (n:Order) WHERE n.x = ''a'' RETURN n')",
	} {
		if err := CheckReadOnlySQL(sql, "nucleus"); err != nil {
			t.Errorf("nucleus: %q refused: %v", sql, err)
		}
	}
}

func TestDecodeUnicodeEscapes(t *testing.T) {
	cases := map[string]string{
		`d\0061t\+000061`: "data",
		`a\\b`:            `a\b`,
		`\D83D\DE00`:      "\U0001F600",
	}
	for in, want := range cases {
		got, err := decodeUnicodeEscapes(in, '\\')
		if err != nil || got != want {
			t.Errorf("%q -> %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{`\zz`, `\00`, `\+00000`, `\D83D`, `\DE00`, `\0000`, `\+110000`} {
		if got, err := decodeUnicodeEscapes(in, '\\'); err == nil {
			t.Errorf("%q decoded to %q; want an error", in, got)
		}
	}
}
