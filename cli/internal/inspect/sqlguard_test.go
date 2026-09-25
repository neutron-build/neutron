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
