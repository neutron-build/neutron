package mcp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/inspect"
)

// These gates run before any statement: env.client is nil, so reaching the
// database would panic — the refusal must come first.

func TestWriteToolUnavailableWithoutAllowWrites(t *testing.T) {
	env := &toolEnv{engine: inspect.Engine{Product: "postgres"}, redactor: inspect.Redactor{Enabled: true}}
	_, err := callTool(context.Background(), env, "execute_sql", map[string]any{"sql": "DELETE FROM t"})
	if err == nil || !strings.Contains(err.Error(), "--allow-writes") {
		t.Fatalf("execute_sql on a read-only server: %v", err)
	}
	if errors.Is(err, errUnknownTool) {
		t.Fatal("write tool reported as unknown instead of unauthorized")
	}
	_, err = callTool(context.Background(), env, "no_such_tool", nil)
	if !errors.Is(err, errUnknownTool) {
		t.Fatalf("unknown tool: %v", err)
	}
}

func TestQuerySQLRefusesWritesBeforeTheDatabase(t *testing.T) {
	for _, product := range []string{"postgres", "nucleus"} {
		for _, allow := range []bool{false, true} {
			env := &toolEnv{engine: inspect.Engine{Product: product}, allowWrites: allow}
			for _, sql := range []string{"DELETE FROM users", "SELECT 1; DROP TABLE users", "SELECT pg_advisory_lock(1)"} {
				_, err := callTool(context.Background(), env, "query_sql", map[string]any{"sql": sql})
				var ge *inspect.GuardError
				if err == nil || (!errors.As(err, &ge) && !strings.Contains(err.Error(), "read-only guard")) {
					t.Errorf("%s allow=%v: %q -> %v", product, allow, sql, err)
				}
				if allow && !strings.Contains(err.Error(), "execute_sql") {
					t.Errorf("%s: refusal does not point at execute_sql: %v", product, err)
				}
			}
		}
	}
	env := &toolEnv{engine: inspect.Engine{Product: "nucleus"}}
	for _, sql := range []string{"SELECT KV_SET('a','b')", "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d", "EXPLAIN ANALYZE SELECT 1"} {
		if _, err := callTool(context.Background(), env, "query_sql", map[string]any{"sql": sql}); err == nil {
			t.Errorf("nucleus: %q allowed", sql)
		}
	}
}

func TestCypherWritesRefused(t *testing.T) {
	env := &toolEnv{engine: inspect.Engine{Product: "nucleus"}}
	for _, q := range []string{"CREATE (n:X)", "MATCH (n) DETACH DELETE n", "MATCH (n) SET n.a = 1"} {
		if _, err := callTool(context.Background(), env, "cypher_query", map[string]any{"query": q}); err == nil || !strings.Contains(err.Error(), "read-only guard") {
			t.Errorf("%q -> %v", q, err)
		}
	}
}

func TestExplainAndPlanRefuseNucleus(t *testing.T) {
	env := &toolEnv{engine: inspect.Engine{Product: "nucleus"}}
	if _, err := callTool(context.Background(), env, "explain_sql", map[string]any{"sql": "SELECT 1"}); err == nil || !strings.Contains(err.Error(), "PostgreSQL only") {
		t.Errorf("explain_sql on nucleus: %v", err)
	}
	if _, err := callTool(context.Background(), env, "plan_schema_changes", map[string]any{"changes": []any{map[string]any{"op": "add-column", "bogus": 1}}}); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Errorf("plan_schema_changes with an unknown field: %v", err)
	}
}

func TestModelsTouched(t *testing.T) {
	got := modelsTouched("SELECT kv_set('a','b'), DOC_INSERT('c','{}'), GRAPH_ADD_NODE('X'), kv_get('a')")
	want := map[string]bool{"kv": true, "document": true, "graph": true}
	if len(got) != len(want) {
		t.Fatalf("models %v", got)
	}
	for _, m := range got {
		if !want[m] {
			t.Errorf("unexpected model %s", m)
		}
	}
	if len(modelsTouched("UPDATE t SET a = 1")) != 0 {
		t.Error("plain SQL attributed to a Nucleus model")
	}
}

func TestEnvelopeCarriesLimitsAndRedaction(t *testing.T) {
	env := &toolEnv{engine: inspect.Engine{Product: "nucleus", Version: inspect.Measured.NucleusVersion}, redactor: inspect.Redactor{Enabled: true}}
	out, err := callToolWith(context.Background(), env, toolSpec{
		def:    toolDef{Name: "fake"},
		access: accessRead,
		handler: func(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
			return &toolOutput{Data: []any{map[string]any{"id": 1, "api_key": "k"}}, Model: "document"}, nil
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.Enforcement != inspect.EnforcedLexically || out.Access != accessRead {
		t.Errorf("enforcement %q access %q", out.Enforcement, out.Access)
	}
	if len(out.Limits) != 1 || out.Limits[0].Model != "document" || out.Limits[0].Transaction != inspect.TxRollbackNotIsolated {
		t.Errorf("limits %+v", out.Limits)
	}
	if len(out.Redacted) != 1 || out.Redacted[0] != "api_key" {
		t.Errorf("redacted %v", out.Redacted)
	}
	row := out.Data.([]any)[0].(map[string]any)
	if row["api_key"] != inspect.RedactedValue {
		t.Errorf("row %v", row)
	}
}

func TestHTTPWritesRequireToken(t *testing.T) {
	t.Setenv("NEUTRON_MCP_TOKEN", "")
	s := &Server{env: &toolEnv{allowWrites: true}}
	err := s.RunHTTP(context.Background(), "127.0.0.1:0")
	if err == nil || !strings.Contains(err.Error(), "NEUTRON_MCP_TOKEN") {
		t.Fatalf("RunHTTP with writes and no token: %v", err)
	}
}

// Review 1 MEDIUM-3: a DNS-rebinding page reaches the listener under its own
// domain name and, once rebound, is same-origin. Such requests are refused;
// requests addressed by loopback name, IP literal or the --host name are not.
func TestHTTPRefusesRebindingHosts(t *testing.T) {
	t.Setenv("NEUTRON_MCP_TOKEN", "")
	s := &Server{env: &toolEnv{}}
	cases := []struct {
		bind, host, origin string
		want               int
	}{
		{"127.0.0.1:7792", "attacker.example:7792", "http://attacker.example:7792", http.StatusForbidden},
		{"127.0.0.1:7792", "attacker.example:7792", "", http.StatusForbidden},
		{"127.0.0.1:7792", "127.0.0.1:7792", "http://attacker.example", http.StatusForbidden},
		{"0.0.0.0:7792", "rebind.attacker.example", "", http.StatusForbidden},
		{"127.0.0.1:7792", "127.0.0.1:7792", "", http.StatusOK},
		{"127.0.0.1:7792", "localhost:7792", "http://localhost:3000", http.StatusOK},
		{"127.0.0.1:7792", "[::1]:7792", "", http.StatusOK},
		{"0.0.0.0:7792", "192.168.1.20:7792", "http://192.168.1.20:7792", http.StatusOK},
		{"devbox:7792", "devbox:7792", "", http.StatusOK},
	}
	for _, c := range cases {
		h, err := s.httpHandler(c.bind)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodGet, "/tools", nil)
		req.Host = c.host
		if c.origin != "" {
			req.Header.Set("Origin", c.origin)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("bind %s Host %q Origin %q: %d, want %d", c.bind, c.host, c.origin, rec.Code, c.want)
		}
	}
}

// Review 1 L1: the agent cannot point migration_status at another directory.
func TestMigrationStatusConfinedToConfiguredDir(t *testing.T) {
	env := &toolEnv{engine: inspect.Engine{Product: "postgres"}, migrationsDir: "migrations"}
	_, err := callTool(context.Background(), env, "migration_status", map[string]any{"dir": "/etc"})
	if err == nil || !strings.Contains(err.Error(), "--migrations") {
		t.Fatalf("dir outside --migrations: %v", err)
	}
	spec, _ := lookupTool("migration_status", false)
	if _, ok := spec.def.InputSchema["properties"].(map[string]any)["dir"]; ok {
		t.Fatalf("migration_status still advertises a dir argument")
	}
}
