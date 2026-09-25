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
