package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/inspect"
)

// TestMCPReadOnlyAndRedactionLive runs the MCP tools against a REAL
// disposable PostgreSQL database with an oracle connection checking that
// nothing changed. Skipped unless NEUTRON_E2E_DATABASE_URL is set
// (NEUTRON_LIVE_REQUIRED=1 fails instead). One x06mcp_* database, dropped.
func TestMCPReadOnlyAndRedactionLive(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; MCP live test skipped")
	}
	ctx := context.Background()
	dbName := fmt.Sprintf("x06mcp_%d_%d", os.Getpid(), time.Now().UnixNano())
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + dbName
	dbURL := u.String()
	admin, err := db.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if strings.HasPrefix(dbName, "x06mcp_") {
			_ = admin.Exec(c, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName))
		}
	})
	oracle, err := db.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer oracle.Close()
	for _, stmt := range []string{
		`CREATE TABLE users (id bigserial PRIMARY KEY, email text NOT NULL, password_hash text, "apiKey" text)`,
		`INSERT INTO users (email, password_hash, "apiKey") VALUES ('a@x.test', 'secret-hash-1', 'k1'), ('b@x.test', NULL, 'k2')`,
	} {
		if err := oracle.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	scalar := func(q string) string {
		var v string
		if err := oracle.QueryRow(ctx, q).Scan(&v); err != nil {
			t.Fatalf("oracle %q: %v", q, err)
		}
		return v
	}
	state := func() string {
		return scalar(`SELECT count(*)::text || '/' || (SELECT last_value::text FROM users_id_seq) || '/' ||
			(SELECT count(*)::text FROM information_schema.tables WHERE table_schema = 'public') FROM users`)
	}

	srv, err := NewServer(ctx, dbURL, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if srv.Engine().Product != "postgres" {
		t.Fatalf("engine %+v", srv.Engine())
	}
	call := func(name string, args map[string]any) (*envelope, error) {
		return callTool(ctx, srv.env, name, args)
	}

	t.Run("fail-before: the previous prefix check admitted these writes", func(t *testing.T) {
		// The pre-X06 query_sql guard (isReadOnlySQL) accepted any text
		// starting with SELECT/EXPLAIN/SHOW/WITH and ran it outside any
		// transaction. Each statement below passed it; run the first one
		// the old way on the oracle and it deletes rows.
		legacy := func(sql string) bool {
			up := strings.TrimSpace(strings.ToUpper(sql))
			return strings.HasPrefix(up, "SELECT") || strings.HasPrefix(up, "EXPLAIN") || strings.HasPrefix(up, "SHOW") || strings.HasPrefix(up, "WITH")
		}
		for _, sql := range bypasses {
			if !legacy(sql) {
				t.Fatalf("legacy check refused %q; the reproduction is wrong", sql)
			}
		}
		if err := oracle.Exec(ctx, `CREATE TABLE scratch (id int)`); err != nil {
			t.Fatal(err)
		}
		if err := oracle.Exec(ctx, `INSERT INTO scratch VALUES (1)`); err != nil {
			t.Fatal(err)
		}
		if err := oracle.Exec(ctx, `WITH d AS (DELETE FROM scratch RETURNING *) SELECT * FROM d`); err != nil {
			t.Fatal(err)
		}
		if n := scalar(`SELECT count(*)::text FROM scratch`); n != "0" {
			t.Fatalf("scratch rows %s", n)
		}
		if err := oracle.Exec(ctx, `DROP TABLE scratch`); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("query_sql refuses every write form, and nothing changes", func(t *testing.T) {
		before := state()
		for _, sql := range bypasses {
			env, err := call("query_sql", map[string]any{"sql": sql})
			if err == nil {
				t.Errorf("%q allowed: %+v", sql, env)
				continue
			}
			if !strings.Contains(err.Error(), "25006") && !strings.Contains(err.Error(), "read-only") {
				t.Errorf("%q refused for an unexpected reason: %v", sql, err)
			}
		}
		if after := state(); after != before {
			t.Fatalf("state changed: %s -> %s", before, after)
		}
	})

	t.Run("review 1 HIGH-1: no session effect outlives a read, and escaping built-ins are refused", func(t *testing.T) {
		advisory := func() string {
			return scalar(`SELECT count(*)::text FROM pg_locks WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`)
		}
		// A user-defined wrapper is invisible to any name check: this is
		// the connection-level defence on its own.
		if err := oracle.Exec(ctx, `CREATE FUNCTION x06_grab(k bigint) RETURNS void LANGUAGE sql AS 'SELECT pg_advisory_lock(k)'`); err != nil {
			t.Fatal(err)
		}
		defer oracle.Exec(context.Background(), `DROP FUNCTION x06_grab(bigint)`) //nolint:errcheck

		// Fail-before: the X06 attempt-1 read path (READ ONLY + ROLLBACK on
		// a pooled connection that is then returned) leaves the lock held.
		old, err := db.Connect(ctx, dbURL)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := old.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "SELECT x06_grab(4343)"); err != nil {
			t.Fatal(err)
		}
		_ = tx.Rollback(ctx)
		conn.Release()
		if n := advisory(); n != "1" {
			t.Fatalf("fail-before: pooled READ ONLY + ROLLBACK left %s advisory locks, want 1 (the reproduction is wrong)", n)
		}
		old.Close()
		waitFor(t, func() bool { return advisory() == "0" })

		// Fixed path: query_sql through the wrapper, then the oracle.
		if _, err := call("query_sql", map[string]any{"sql": "SELECT x06_grab(4343)"}); err != nil {
			t.Fatal(err)
		}
		if n := advisory(); n != "0" {
			t.Fatalf("query_sql left %s advisory locks held", n)
		}
		// Every read path shares it: several reads, still nothing held and
		// the server keeps working (the pool reconnects).
		for i := 0; i < 3; i++ {
			if _, err := call("query_sql", map[string]any{"sql": fmt.Sprintf("SELECT x06_grab(%d)", 5000+i)}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := call("list_tables", nil); err != nil {
			t.Fatal(err)
		}
		if n := advisory(); n != "0" {
			t.Fatalf("%s advisory locks held after repeated reads", n)
		}

		statsReset := func() string {
			return scalar(`SELECT coalesce(stats_reset::text, 'never') FROM pg_stat_database WHERE datname = current_database()`)
		}
		stats := statsReset()
		for _, sql := range guardBypasses {
			_, err := call("query_sql", map[string]any{"sql": sql})
			if err == nil || !strings.Contains(err.Error(), "read-only guard") {
				t.Errorf("%q -> %v", sql, err)
			}
		}
		if n := advisory(); n != "0" {
			t.Fatalf("bypass attempts left %s advisory locks", n)
		}
		if got := statsReset(); got != stats {
			t.Fatalf("pg_stat_database.stats_reset moved: %s -> %s", stats, got)
		}

		// A server with standard_conforming_strings = off ends '\'' at the
		// second quote; the guard reads the text that way too.
		if err := admin.Exec(ctx, fmt.Sprintf(`ALTER DATABASE %q SET standard_conforming_strings = off`, dbName)); err != nil {
			t.Fatal(err)
		}
		defer admin.Exec(context.Background(), fmt.Sprintf(`ALTER DATABASE %q RESET standard_conforming_strings`, dbName)) //nolint:errcheck
		if _, err := call("query_sql", map[string]any{"sql": `SELECT '\'', pg_advisory_lock(5151) --'`}); err == nil || !strings.Contains(err.Error(), "read-only guard") {
			t.Fatalf("standard_conforming_strings=off form -> %v", err)
		}
		if n := advisory(); n != "0" {
			t.Fatalf("%s advisory locks held", n)
		}
	})

	t.Run("reads are redacted by name, with the redaction reported", func(t *testing.T) {
		env, err := call("query_sql", map[string]any{"sql": `SELECT id, email, password_hash, "apiKey" FROM users ORDER BY id`})
		if err != nil {
			t.Fatal(err)
		}
		if env.Access != accessRead || env.Enforcement != inspect.EnforcedByEngine {
			t.Fatalf("access %q enforcement %q", env.Access, env.Enforcement)
		}
		rows := env.Data.([]any)
		first, second := rows[0].(map[string]any), rows[1].(map[string]any)
		if first["email"] != "a@x.test" || first["password_hash"] != inspect.RedactedValue || first["apiKey"] != inspect.RedactedValue {
			t.Fatalf("row 1 %v", first)
		}
		if second["password_hash"] != nil {
			t.Fatalf("NULL redacted: %v", second)
		}
		if strings.Join(env.Redacted, ",") != "apiKey,password_hash" {
			t.Fatalf("redacted %v", env.Redacted)
		}
		if len(env.Limits) != 1 || env.Limits[0].Model != "sql" || env.Limits[0].Transaction != inspect.TxAtomic {
			t.Fatalf("limits %+v", env.Limits)
		}
		// The JSON-RPC answer carries the same envelope as text and as
		// structuredContent.
		raw, _ := json.Marshal(map[string]any{"name": "query_sql", "arguments": map[string]any{"sql": "SELECT password_hash FROM users WHERE id = 1"}})
		resp := srv.handleToolCall(ctx, raw)
		result := resp.Result.(map[string]any)
		text := result["content"].([]map[string]any)[0]["text"].(string)
		if strings.Contains(text, "secret-hash-1") || !strings.Contains(text, `"redacted"`) || result["structuredContent"] == nil {
			t.Fatalf("rpc result %v", result)
		}
		// Operator opt-out.
		srv.Configure(Options{NoRedact: true})
		env, err = call("query_sql", map[string]any{"sql": "SELECT password_hash FROM users WHERE id = 1"})
		srv.Configure(Options{})
		if err != nil || env.Data.([]any)[0].(map[string]any)["password_hash"] != "secret-hash-1" {
			t.Fatalf("--no-redact: %v %v", env, err)
		}
	})

	t.Run("inspection and planning tools change nothing", func(t *testing.T) {
		before := state()
		env, err := call("engine_limits", nil)
		if err != nil {
			t.Fatal(err)
		}
		rep := env.Data.(inspect.Report)
		if sql, _ := rep.Model("sql"); sql.Transaction != inspect.TxAtomic || rep.Live == nil || rep.Live.Fsync == "" {
			t.Fatalf("engine_limits %+v", rep)
		}
		env, err = call("inspect_table", map[string]any{"table": "users"})
		if err != nil {
			t.Fatal(err)
		}
		j := env.Data.(*inspect.Journey)
		rows := j.Stage(inspect.StageRows).Data.(inspect.RowsData)
		for _, r := range rows.Rows {
			for ci, c := range rows.Columns {
				if (c == "password_hash" || c == "apiKey") && r[ci] != nil && r[ci] != inspect.RedactedValue {
					t.Fatalf("inspect_table leaked %s: %v", c, r[ci])
				}
			}
		}
		if strings.Join(env.Redacted, ",") != "apiKey,password_hash" {
			t.Fatalf("inspect_table redacted %v", env.Redacted)
		}
		env, err = call("migration_status", nil)
		if err != nil {
			t.Fatal(err)
		}
		if env.Data.(inspect.MigrationsData).History != db.HistoryAbsent.String() {
			t.Fatalf("migration_status %+v", env.Data)
		}
		env, err = call("explain_sql", map[string]any{"sql": "SELECT * FROM users WHERE id = 1"})
		if err != nil {
			t.Fatal(err)
		}
		if env.Data.(map[string]any)["executed"] != false {
			t.Fatalf("explain %+v", env.Data)
		}
		if _, err := call("explain_sql", map[string]any{"sql": "DELETE FROM users"}); err == nil {
			t.Fatal("explain_sql planned a DELETE")
		}
		env, err = call("plan_schema_changes", map[string]any{"changes": []any{
			map[string]any{"op": "add-column", "schema": "public", "table": "users", "column": "nickname", "type": "text"},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if env.Access != accessPlan {
			t.Fatalf("plan access %q", env.Access)
		}
		b, _ := json.Marshal(env.Data)
		if !strings.Contains(strings.ToLower(string(b)), "add column \\\"nickname\\\"") && !strings.Contains(strings.ToLower(string(b)), "add column nickname") {
			t.Fatalf("plan statements %s", b)
		}
		if n := scalar(`SELECT count(*)::text FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'nickname'`); n != "0" {
			t.Fatal("plan_schema_changes applied the change")
		}
		if after := state(); after != before {
			t.Fatalf("state changed: %s -> %s", before, after)
		}
	})

	t.Run("execute_sql exists only with --allow-writes and commits with the limits attached", func(t *testing.T) {
		if _, err := call("execute_sql", map[string]any{"sql": "INSERT INTO users (email) VALUES ('c@x.test')"}); err == nil {
			t.Fatal("execute_sql ran on a read-only server")
		}
		srv.Configure(Options{AllowWrites: true})
		defer srv.Configure(Options{})
		if _, err := call("execute_sql", map[string]any{"sql": "INSERT INTO users (email) VALUES ('x'); DELETE FROM users"}); err == nil {
			t.Fatal("execute_sql ran two statements")
		}
		env, err := call("execute_sql", map[string]any{"sql": "INSERT INTO users (email) VALUES ('c@x.test') RETURNING id, email"})
		if err != nil {
			t.Fatal(err)
		}
		data := env.Data.(map[string]any)
		if env.Access != accessWrite || data["rowsAffected"] != int64(1) || data["committed"] != true {
			t.Fatalf("execute_sql %+v", env)
		}
		if n := scalar(`SELECT count(*)::text FROM users WHERE email = 'c@x.test'`); n != "1" {
			t.Fatalf("oracle rows %s", n)
		}
		if len(env.Limits) == 0 || env.Limits[0].Transaction != inspect.TxAtomic {
			t.Fatalf("limits %+v", env.Limits)
		}
		// A failing statement leaves nothing behind.
		if _, err := call("execute_sql", map[string]any{"sql": "INSERT INTO users (id, email) VALUES (1, 'dup')"}); err == nil {
			t.Fatal("duplicate key accepted")
		}
		// Even with writes allowed, query_sql stays read-only.
		if _, err := call("query_sql", map[string]any{"sql": "WITH d AS (DELETE FROM users RETURNING *) SELECT * FROM d"}); err == nil {
			t.Fatal("query_sql wrote with --allow-writes")
		}
		if n := scalar(`SELECT count(*)::text FROM users`); n != "3" {
			t.Fatalf("oracle users %s", n)
		}
	})
}

// guardBypasses are the review 1 forms that passed the attempt-1 guard on
// PostgreSQL (plus the \r-comment form found while fixing them).
var guardBypasses = []string{
	"SELECT pg_advisory_lock\v(4343)",
	`SELECT U&"pg_\0061dvisory_lock"(4545)`,
	`SELECT U&"pg_!0061dvisory_lock" UESCAPE '!' (4545)`,
	"SELECT pg_catalog.pg_advisory_lock\v(4646), pg_terminate_backend\v(-1)",
	"SELECT 1 --x\r, pg_advisory_lock(5050)",
	"SELECT PG_ADVISORY_LOCK /* c */ (4747)",
	"SELECT pg_stat_reset()",
	"SELECT pg_stat_reset\v()",
	"SELECT pg_logical_emit_message(false, 'x06', 'escaped')",
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	for i := 0; i < 50; i++ {
		if ok() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("condition not reached in 5s")
}

// bypasses are statements the pre-X06 prefix guard admitted that write.
var bypasses = []string{
	"WITH d AS (DELETE FROM users RETURNING *) SELECT * FROM d",
	"SELECT * INTO users_copy FROM users",
	"SELECT nextval('users_id_seq')",
	"EXPLAIN ANALYZE DELETE FROM users",
	"WITH u AS (UPDATE users SET email = 'x' RETURNING *) SELECT count(*) FROM u",
	"SELECT pg_advisory_lock(42)",
}
