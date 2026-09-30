package mcp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/inspect"
)

// TestMCPNucleusLive exercises the lexical guard in addition to the
// measured engine READ ONLY protection. Specialty models are written through
// ordinary SELECTs, so retain the guard for unverified mutation paths.
// Runs against a disposable engine named by
// NEUTRON_E2E_NUCLEUS_URL; skipped when unset.
func TestMCPNucleusLive(t *testing.T) {
	nurl := os.Getenv("NEUTRON_E2E_NUCLEUS_URL")
	if nurl == "" {
		if os.Getenv("NEUTRON_NUCLEUS_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_NUCLEUS_LIVE_REQUIRED=1 but NEUTRON_E2E_NUCLEUS_URL is not set")
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

// READ ONLY protects NEXTVAL through these measured wrapper forms. Each
// raw check begins a clean transaction: an old aborted state must not mask
// whether the statement itself was refused before changing the sequence.
func TestMCPNucleusReadOnlyRefusesWrappedNextval(t *testing.T) {
	nurl := os.Getenv("NEUTRON_E2E_NUCLEUS_URL")
	if nurl == "" {
		if os.Getenv("NEUTRON_NUCLEUS_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_NUCLEUS_LIVE_REQUIRED=1 but NEUTRON_E2E_NUCLEUS_URL is not set")
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

	for _, kind := range []string{"view", "subquery", "where", "tool-view"} {
		t.Run(kind, func(t *testing.T) {
			name := fmt.Sprintf("x06readonly_%d", time.Now().UnixNano())
			seq, view := name+"_seq", name+"_v"
			if err := oracle.Exec(ctx, "CREATE SEQUENCE "+seq); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := oracle.Exec(ctx, "DROP SEQUENCE "+seq); err != nil {
					t.Error(err)
				}
			}()
			statement := fmt.Sprintf("SELECT (SELECT NEXTVAL('%s')) AS n", seq)
			switch kind {
			case "view", "tool-view":
				if err := oracle.Exec(ctx, fmt.Sprintf("CREATE VIEW %s AS SELECT NEXTVAL('%s') AS n", view, seq)); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := oracle.Exec(ctx, "DROP VIEW "+view); err != nil {
						t.Error(err)
					}
				}()
				statement = "SELECT * FROM " + view
			case "where":
				statement = fmt.Sprintf("SELECT 1 AS n WHERE NEXTVAL('%s') > 0", seq)
			}
			if kind == "tool-view" {
				_, err := callTool(ctx, srv.env, "query_sql", map[string]any{"sql": statement})
				// pgx's extended path can expose the subsequent aborted-transaction
				// refusal after Describe encounters the protected NEXTVAL. The raw
				// checks above establish the original 25006 and the effect check below
				// independently establishes that this tool call did not advance it.
				if err == nil || (!strings.Contains(err.Error(), "25006") && !strings.Contains(err.Error(), "25P02")) {
					t.Fatalf("wrapped NEXTVAL must be refused by READ ONLY, got %v", err)
				}
				if _, err := callTool(ctx, srv.env, "query_sql", map[string]any{"sql": "SELECT 1 AS n"}); err != nil {
					t.Fatalf("clean read after refusal: %v", err)
				}
			} else {
				conn, err := oracle.Acquire(ctx)
				if err != nil {
					t.Fatal(err)
				}
				tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
				if err != nil {
					conn.Release()
					t.Fatal(err)
				}
				_, queryErr := tx.Exec(ctx, statement, pgx.QueryExecModeSimpleProtocol)
				rollbackErr := tx.Rollback(ctx)
				conn.Release()
				if rollbackErr != nil {
					t.Fatal(rollbackErr)
				}
				var pgErr *pgconn.PgError
				if !errors.As(queryErr, &pgErr) || pgErr.Code != "25006" {
					t.Fatalf("fresh READ ONLY must refuse NEXTVAL with 25006, got %v", queryErr)
				}
			}
			var next int64
			if err := oracle.QueryRow(ctx, fmt.Sprintf("SELECT NEXTVAL('%s')", seq)).Scan(&next); err != nil {
				t.Fatal(err)
			}
			if next != 1 {
				t.Fatalf("refused NEXTVAL changed sequence: next=%d, want1", next)
			}
		})
	}
	for _, spec := range toolSpecs() {
		if spec.def.Name == "query_sql" && (!strings.Contains(spec.def.Description, "READ ONLY") || !strings.Contains(spec.def.Description, "unverified specialty")) {
			t.Fatalf("query_sql must describe bounded READ ONLY protection: %q", spec.def.Description)
		}
	}
}
