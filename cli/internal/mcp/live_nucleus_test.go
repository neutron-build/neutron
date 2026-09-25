package mcp

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/inspect"
)

// TestMCPNucleusLive exercises the read-only guard where it is the ONLY
// enforcement: Nucleus does not apply READ ONLY (capability report
// txn.read_only_rejects_writes), and its models are written through
// ordinary SELECTs. Runs against a disposable engine named by
// NEUTRON_E2E_NUCLEUS_URL; skipped when unset.
func TestMCPNucleusLive(t *testing.T) {
	nurl := os.Getenv("NEUTRON_E2E_NUCLEUS_URL")
	if nurl == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_NUCLEUS_URL is not set")
		}
		t.Skip("NEUTRON_E2E_NUCLEUS_URL not set")
	}
	ctx := context.Background()
	oracle, err := db.Connect(ctx, nurl)
	if err != nil {
		t.Fatal(err)
	}
	defer oracle.Close()
	srv, err := NewServer(ctx, nurl, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if srv.Engine().Product != "nucleus" {
		t.Fatalf("engine %+v", srv.Engine())
	}
	call := func(name string, args map[string]any) (*envelope, error) { return callTool(ctx, srv.env, name, args) }
	key := fmt.Sprintf("x06_mcp_%d", time.Now().UnixNano())
	table := key + "_t"
	if err := oracle.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, secret_token text)", table)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = oracle.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })
	if err := oracle.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'tok-1')", table)); err != nil {
		t.Fatal(err)
	}
	counts := func() string {
		var nodes, rows int64
		var kv *string
		_ = oracle.QueryRow(ctx, "SELECT GRAPH_NODE_COUNT()").Scan(&nodes)
		_ = oracle.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&rows)
		_ = oracle.QueryRow(ctx, "SELECT KV_GET($1)", key).Scan(&kv)
		v := "nil"
		if kv != nil {
			v = *kv
		}
		return fmt.Sprintf("nodes=%d rows=%d kv=%s", nodes, rows, v)
	}

	before := counts()
	for _, sql := range []string{
		fmt.Sprintf("SELECT KV_SET('%s', 'v')", key),
		fmt.Sprintf("select pg_catalog.kv_set('%s', 'v')", key),
		"SELECT GRAPH_ADD_NODE('X')",
		fmt.Sprintf("WITH d AS (DELETE FROM %s RETURNING *) SELECT * FROM d", table),
		fmt.Sprintf("SELECT * INTO %s_copy FROM %s", table, table),
		fmt.Sprintf("SELECT * FROM %s FOR UPDATE", table),
	} {
		if _, err := call("query_sql", map[string]any{"sql": sql}); err == nil || !strings.Contains(err.Error(), "read-only guard") {
			t.Errorf("%q -> %v", sql, err)
		}
	}
	for _, q := range []string{"CREATE (n:X)", "MATCH (n) DETACH DELETE n"} {
		if _, err := call("cypher_query", map[string]any{"query": q}); err == nil {
			t.Errorf("cypher %q allowed", q)
		}
	}
	if after := counts(); after != before {
		t.Fatalf("guarded reads wrote: %s -> %s", before, after)
	}

	// Review 1 HIGH-1 / HIGH-2 on the engine where the guard is the only
	// enforcement. Fail-before: the engine itself runs each form (a \v
	// before '(' is whitespace to sqlparser; GRAPH_QUERY executes CREATE),
	// shown on the oracle inside a rolled-back transaction or on a
	// throwaway sequence.
	seq := key + "_seq"
	probeSeq := key + "_probe"
	for _, stmt := range []string{"CREATE SEQUENCE " + seq, "CREATE SEQUENCE " + probeSeq} {
		if err := oracle.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_ = oracle.Exec(context.Background(), "DROP SEQUENCE IF EXISTS "+seq)
		_ = oracle.Exec(context.Background(), "DROP SEQUENCE IF EXISTS "+probeSeq)
	})
	var probed int64
	if err := oracle.QueryRow(ctx, "SELECT NEXTVAL\v($1)", probeSeq).Scan(&probed); err != nil || probed != 1 {
		t.Fatalf("fail-before: engine did not run NEXTVAL\\v(...): %d %v", probed, err)
	}
	nodes := func() int64 {
		var n int64
		if err := oracle.QueryRow(ctx, "SELECT GRAPH_NODE_COUNT()").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	nodesBefore := nodes()
	tx, err := oracle.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var created string
	if err := tx.QueryRow(ctx, "SELECT GRAPH_QUERY('CREATE (n:X06R2 {a: 1})')").Scan(&created); err != nil || !strings.Contains(created, "node_0") {
		t.Fatalf("fail-before: engine did not run write Cypher through GRAPH_QUERY: %q %v", created, err)
	}
	_ = tx.Rollback(ctx)
	if n := nodes(); n != nodesBefore {
		t.Fatalf("probe node not rolled back: %d -> %d", nodesBefore, n)
	}

	before = counts()
	for _, sql := range []string{
		fmt.Sprintf("SELECT NEXTVAL\v('%s')", seq),
		fmt.Sprintf("SELECT NeXtVaL /* c */ ('%s')", seq),
		fmt.Sprintf("SELECT 1 --c\r, NEXTVAL('%s')", seq),
		fmt.Sprintf("SELECT KV_SET\v('%s', 'v')", key),
		fmt.Sprintf(`SELECT U&"kv_\0073et"('%s', 'v')`, key),
		"SELECT DATALOG_ASSERT\v('x06r2(a)')",
		"SELECT FTS_INDEX\v(990001, 'x06r2')",
		"SELECT BLOB_STORE\v('x06r2', 'x')",
		"SELECT TS_INSERT\v('x06r2', 1, 2)",
		"SELECT GRAPH_QUERY('CREATE (n:X06R2 {a: 1})')",
		"SELECT graph_query\v('MATCH (n) DETACH DELETE n')",
		"SELECT GRAPH_QUERY($$CREATE (n:X06R2)$$)",
		"SELECT GRAPH_QUERY('MATCH (n) RETURN n' || ' CREATE (m:X06R2)')",
	} {
		if _, err := call("query_sql", map[string]any{"sql": sql}); err == nil || !strings.Contains(err.Error(), "read-only guard") {
			t.Errorf("%q -> %v", sql, err)
		}
	}
	if after := counts(); after != before {
		t.Fatalf("guarded reads wrote: %s -> %s", before, after)
	}
	// A read-only GRAPH_QUERY literal still works through query_sql.
	if env, err := call("query_sql", map[string]any{"sql": "SELECT GRAPH_QUERY('MATCH (n:X06R2) RETURN n')"}); err != nil || len(env.Data.([]any)) != 1 {
		t.Fatalf("read-only GRAPH_QUERY: %v %v", env, err)
	}
	var next int64
	if err := oracle.QueryRow(ctx, "SELECT NEXTVAL($1)", seq).Scan(&next); err != nil || next != 1 {
		t.Fatalf("sequence advanced by guarded reads: next %d %v", next, err)
	}
	if n := nodes(); n != nodesBefore {
		t.Fatalf("graph changed: %d -> %d", nodesBefore, n)
	}

	env, err := call("query_sql", map[string]any{"sql": "SELECT id, secret_token FROM " + table})
	if err != nil {
		t.Fatal(err)
	}
	if env.Enforcement != inspect.EnforcedLexically {
		t.Fatalf("enforcement %q", env.Enforcement)
	}
	if env.Data.([]any)[0].(map[string]any)["secret_token"] != inspect.RedactedValue {
		t.Fatalf("secret not redacted: %v", env.Data)
	}
	if len(env.Limits) != 1 || env.Limits[0].Transaction != inspect.TxPartial {
		t.Fatalf("limits %+v", env.Limits)
	}

	env, err = call("inspect_table", map[string]any{"table": table})
	if err != nil {
		t.Fatal(err)
	}
	j := env.Data.(*inspect.Journey)
	if j.Stage(inspect.StageRows).Status != inspect.StageAvailable || j.Stage(inspect.StageSchema).Status != inspect.StageUnavailable {
		t.Fatalf("journey stages %+v", j.Stages)
	}

	srv.Configure(Options{AllowWrites: true})
	env, err = call("execute_sql", map[string]any{"sql": fmt.Sprintf("SELECT KV_SET('%s', 'v')", key)})
	srv.Configure(Options{})
	if err != nil {
		t.Fatal(err)
	}
	models := map[string]inspect.ModelLimits{}
	for _, l := range env.Limits {
		models[l.Model] = l
	}
	if kv, ok := models["kv"]; !ok || kv.AtomicWithSQL != inspect.Unsupported || len(kv.Warnings) == 0 {
		t.Fatalf("execute_sql limits %+v", env.Limits)
	}
	var got *string
	if err := oracle.QueryRow(ctx, "SELECT KV_GET($1)", key).Scan(&got); err != nil || got == nil || *got != "v" {
		t.Fatalf("oracle KV_GET after execute_sql: %v %v", got, err)
	}
	_ = oracle.Exec(ctx, "SELECT KV_DEL($1)", key)
}

// TestMCPNucleusWrappedMutatorGapIsDocumented pins a documented limitation
// (review 2): Nucleus applies no READ ONLY and has no rollback or connection
// reset covering the guard, so a view or routine that wraps a mutating
// function is not caught by the name check and its write persists. The test
// asserts the gap exists AND that the tool says so; if the engine or guard
// ever closes it, this fails and the docs should be updated to match.
func TestMCPNucleusWrappedMutatorGapIsDocumented(t *testing.T) {
	nurl := os.Getenv("NEUTRON_E2E_NUCLEUS_URL")
	if nurl == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_NUCLEUS_URL is not set")
		}
		t.Skip("NEUTRON_E2E_NUCLEUS_URL not set")
	}
	ctx := context.Background()
	oracle, err := db.Connect(ctx, nurl)
	if err != nil {
		t.Fatal(err)
	}
	defer oracle.Close()
	srv, err := NewServer(ctx, nurl, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	name := fmt.Sprintf("x06gap_%d", time.Now().UnixNano())
	seq, view := name+"_seq", name+"_v"
	for _, stmt := range []string{"CREATE SEQUENCE " + seq, fmt.Sprintf("CREATE VIEW %s AS SELECT NEXTVAL('%s') AS n", view, seq)} {
		if err := oracle.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_ = oracle.Exec(context.Background(), "DROP VIEW IF EXISTS "+view)
		_ = oracle.Exec(context.Background(), "DROP SEQUENCE IF EXISTS "+seq)
	})

	if _, err := callTool(ctx, srv.env, "query_sql", map[string]any{"sql": "SELECT * FROM " + view}); err != nil {
		t.Fatalf("gap closed? the wrapped mutator is now refused (%v): update the Nucleus limitation docs", err)
	}
	var next int64
	if err := oracle.QueryRow(ctx, fmt.Sprintf("SELECT NEXTVAL('%s')", seq)).Scan(&next); err != nil {
		t.Fatal(err)
	}
	if next < 2 {
		t.Fatalf("gap closed? the wrapped NEXTVAL did not persist (next=%d): update the Nucleus limitation docs", next)
	}

	for _, s := range toolSpecs() {
		if s.def.Name == "query_sql" && !strings.Contains(s.def.Description, "best-effort") {
			t.Fatalf("query_sql description must state the Nucleus read-only default is best-effort: %q", s.def.Description)
		}
	}
}
