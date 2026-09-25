package studio

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestStudioSQLEditorE2E is the S04 leg of V15: the SQL editor's HTTP
// conversation through the REAL route table and corsMiddleware against a
// REAL disposable Postgres database, with an independent SQL oracle on a
// separate connection.
//
//   - A real long statement (pg_sleep) is cancelled with pg_cancel_backend
//     on the backend that runs it; the oracle observes that exact backend
//     go from active to idle (not terminated), and the next request runs on
//     the SAME backend (the Studio pool is capped at one connection, so
//     reuse is forced and observable through pg_backend_pid()).
//   - The cancel side channel works while that single-connection pool is
//     exhausted, and a request queued behind it can be cancelled before it
//     is ever sent.
//   - EXPLAIN never executes mutations (row counts, sequence state and the
//     catalog are checked by the oracle); EXPLAIN ANALYZE refuses writes
//     unless allowWrites, and with allowWrites the writes are rolled back.
//   - Bound parameters, request IDs, a client disconnect cancelling
//     server-side, and saved queries continuing to work.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server; NEUTRON_LIVE_REQUIRED=1 turns a missing URL into a failure. Uses
// one uniquely named s04_* database, dropped afterwards.
func TestStudioSQLEditorE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio SQL editor e2e skipped")
	}

	dbName := fmt.Sprintf("s04_%d_%d", os.Getpid(), time.Now().Unix())
	dbURL := deriveStudioDatabaseURL(t, base, dbName)

	admin, err := db.Connect(context.Background(), base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatalf("create database %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := admin.Exec(ctx, fmt.Sprintf(
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()`, dbName,
		)); err != nil {
			t.Errorf("terminate backends: %v", err)
		}
		if !strings.HasPrefix(dbName, "s04_") {
			t.Errorf("refusing to drop unexpected database %q", dbName)
			return
		}
		if err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})

	// The oracle: an independent connection, never the Studio pool.
	oracleClient, err := db.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect oracle: %v", err)
	}
	defer oracleClient.Close()
	for _, stmt := range []string{
		`CREATE TABLE memo (id int PRIMARY KEY, body text NOT NULL)`,
		`INSERT INTO memo VALUES (1, 'a'), (2, 'b'), (3, 'c')`,
		`CREATE SEQUENCE memo_seq START 100`,
	} {
		if err := oracleClient.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	oracle := func(query string, dest ...any) {
		t.Helper()
		if err := oracleClient.QueryRow(context.Background(), query).Scan(dest...); err != nil {
			t.Fatalf("oracle %q: %v", query, err)
		}
	}
	memoCount := func() int {
		t.Helper()
		var n int
		oracle(`SELECT count(*) FROM memo`, &n)
		return n
	}
	seqState := func() string {
		t.Helper()
		var last int64
		var called bool
		oracle(`SELECT last_value, is_called FROM memo_seq`, &last, &called)
		return fmt.Sprintf("%d/%v", last, called)
	}

	// Studio's own client: ONE pooled connection, so "the next request runs
	// on the same backend" is a forced, observable property.
	studioURL := dbURL + "?pool_max_conns=1"
	if strings.Contains(dbURL, "?") {
		studioURL = dbURL + "&pool_max_conns=1"
	}
	studioClient, err := db.Connect(context.Background(), studioURL)
	if err != nil {
		t.Fatalf("connect studio client: %v", err)
	}
	defer studioClient.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	s := &Server{
		port:         port,
		sessionToken: "s04-editor-token",
		clients:      map[string]*db.Client{"e2e": studioClient},
		saved:        &savedQueryStore{path: filepath.Join(t.TempDir(), "studio-saved.json")},
	}
	mux, err := s.routes()
	if err != nil {
		t.Fatalf("routes: %v", err)
	}
	ts := httptest.NewUnstartedServer(s.corsMiddleware(mux))
	ts.Listener = ln
	ts.Start()
	defer ts.Close()
	baseURL := ts.URL
	origin := baseURL
	httpClient := &http.Client{Timeout: 90 * time.Second}

	var token string
	doCtx := func(ctx context.Context, method, path, tok, originHeader, body string) (int, map[string]any) {
		t.Helper()
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, rd)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if originHeader != "" {
			req.Header.Set("Origin", originHeader)
		}
		if tok != "" {
			req.Header.Set(sessionHeader, tok)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := httpClient.Do(req)
		if err != nil {
			return -1, map[string]any{"transportError": err.Error()}
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(res.Body)
		var parsed map[string]any
		if len(raw) > 0 && strings.Contains(res.Header.Get("Content-Type"), "json") {
			dec := json.NewDecoder(strings.NewReader(string(raw)))
			dec.UseNumber()
			if err := dec.Decode(&parsed); err != nil {
				t.Errorf("decode %s %s body %q: %v", method, path, raw, err)
			}
		}
		return res.StatusCode, parsed
	}
	post := func(path string, payload any) (int, map[string]any) {
		t.Helper()
		return doCtx(context.Background(), http.MethodPost, path, token, origin, mustJSON(payload))
	}

	code, body := doCtx(context.Background(), http.MethodGet, "/api/session", "", origin, "")
	if code != http.StatusOK {
		t.Fatalf("session: %d %v", code, body)
	}
	token, _ = body["token"].(string)

	// waitActive polls the oracle until a statement carrying marker is
	// running and returns its backend pid.
	waitActive := func(marker string) int {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			var pid int
			err := oracleClient.QueryRow(context.Background(),
				`SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND state = 'active' AND query LIKE '%' || $1 || '%' AND pid <> pg_backend_pid()`,
				marker).Scan(&pid)
			if err == nil {
				return pid
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("statement %s never became active", marker)
		return 0
	}
	backendState := func(pid int) string {
		t.Helper()
		var state string
		err := oracleClient.QueryRow(context.Background(),
			`SELECT coalesce(state, '') FROM pg_stat_activity WHERE pid = $1`, pid).Scan(&state)
		if err != nil {
			return "gone"
		}
		return state
	}
	waitIdle := func(pid int) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if backendState(pid) == "idle" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("backend %d did not return to idle (state %q)", pid, backendState(pid))
	}
	backendPID := func() int {
		t.Helper()
		code, body := post("/api/query", map[string]any{"connectionId": "e2e", "sql": "SELECT pg_backend_pid()"})
		if code != http.StatusOK || body["error"] != nil {
			t.Fatalf("pg_backend_pid query: %d %v", code, body)
		}
		rows := body["rows"].([]any)
		n, _ := rows[0].([]any)[0].(json.Number).Int64()
		return int(n)
	}

	type asyncResult struct {
		code    int
		body    map[string]any
		elapsed time.Duration
	}
	startAsync := func(ctx context.Context, path string, payload any) chan asyncResult {
		ch := make(chan asyncResult, 1)
		raw := mustJSON(payload)
		go func() {
			start := time.Now()
			code, body := doCtx(ctx, http.MethodPost, path, token, origin, raw)
			ch <- asyncResult{code, body, time.Since(start)}
		}()
		return ch
	}
	await := func(ch chan asyncResult, within time.Duration) asyncResult {
		t.Helper()
		select {
		case r := <-ch:
			return r
		case <-time.After(within):
			t.Fatalf("request did not return within %s", within)
			return asyncResult{}
		}
	}

	t.Run("bound parameters and request ids", func(t *testing.T) {
		code, body := post("/api/query", map[string]any{
			"connectionId": "e2e", "requestId": "s04-params-0001",
			"sql":    `SELECT $1::int + 1 AS n, $2::text IS NULL AS was_null, $3::int8 AS big, body FROM memo WHERE id = $4`,
			"params": []any{"41", nil, "9007199254740993", "2"},
		})
		if code != http.StatusOK || body["error"] != nil || body["requestId"] != "s04-params-0001" {
			t.Fatalf("param query: %d %v", code, body)
		}
		row := body["rows"].([]any)[0].([]any)
		if row[0] != json.Number("42") || row[1] != true || row[3] != "b" {
			t.Errorf("param row = %v", row)
		}
		big, _ := row[2].(map[string]any)
		if big["t"] != "int8" || big["v"] != "9007199254740993" {
			t.Errorf("int8 parameter lost precision: %v", row[2])
		}
		// A parameter is data, never SQL.
		code, body = post("/api/query", map[string]any{
			"connectionId": "e2e", "sql": `SELECT count(*) FROM memo WHERE body = $1`,
			"params": []any{"x'; DELETE FROM memo; --"},
		})
		if code != http.StatusOK || body["error"] != nil {
			t.Fatalf("injection-shaped parameter: %d %v", code, body)
		}
		if memoCount() != 3 {
			t.Fatal("a parameter executed as SQL")
		}
		// A wrong parameter count is the server's error, reported honestly.
		code, body = post("/api/query", map[string]any{"connectionId": "e2e", "sql": `SELECT $1::int, $2::int`, "params": []any{"1"}})
		if code != http.StatusOK || body["error"] == nil {
			t.Errorf("parameter count mismatch should report the server error: %d %v", code, body)
		}
	})

	t.Run("cancel a real long query with pg_cancel_backend and reuse its connection", func(t *testing.T) {
		pidBefore := backendPID()
		const marker = "s04_cancel_marker_1"
		reqID := "s04-cancel-0001"
		ch := startAsync(context.Background(), "/api/query", map[string]any{
			"connectionId": "e2e", "requestId": reqID,
			"sql": "SELECT pg_sleep(60) /* " + marker + " */",
		})
		pid := waitActive(marker)
		if pid != pidBefore {
			t.Fatalf("single-connection pool ran the statement on backend %d, expected %d", pid, pidBefore)
		}

		// Refusals do not cancel anything: the statement keeps running.
		code, body := doCtx(context.Background(), http.MethodPost, "/api/query/cancel", "", origin, mustJSON(map[string]any{"connectionId": "e2e", "requestId": reqID}))
		if code != http.StatusForbidden {
			t.Errorf("cancel without token: %d %v", code, body)
		}
		code, body = doCtx(context.Background(), http.MethodPost, "/api/query/cancel", token, "http://localhost:1", mustJSON(map[string]any{"connectionId": "e2e", "requestId": reqID}))
		if code != http.StatusForbidden {
			t.Errorf("cancel from foreign origin: %d %v", code, body)
		}
		code, body = post("/api/query/cancel", map[string]any{"connectionId": "other", "requestId": reqID})
		if code != http.StatusNotFound {
			t.Errorf("cancel through another connection id: %d %v", code, body)
		}
		if st := backendState(pid); st != "active" {
			t.Fatalf("refused cancels must not stop the statement; backend state %q", st)
		}

		// While the only pooled connection is busy, a second request with the
		// same ID is refused immediately, and a request queued for the pool
		// can be cancelled before it is ever sent.
		code, body = post("/api/query", map[string]any{"connectionId": "e2e", "requestId": reqID, "sql": "SELECT 1"})
		if code != http.StatusConflict {
			t.Errorf("duplicate running request id: %d %v", code, body)
		}
		queued := startAsync(context.Background(), "/api/query", map[string]any{
			"connectionId": "e2e", "requestId": "s04-queued-0001", "sql": "INSERT INTO memo VALUES (99, 'queued')",
		})
		time.Sleep(200 * time.Millisecond)
		code, body = post("/api/query/cancel", map[string]any{"connectionId": "e2e", "requestId": "s04-queued-0001"})
		if code != http.StatusOK || body["state"] != "canceled-before-dispatch" {
			t.Errorf("cancel queued request: %d %v", code, body)
		}
		qr := await(queued, 10*time.Second)
		if qr.body["canceled"] != true {
			t.Errorf("queued request result: %d %v", qr.code, qr.body)
		}

		// The real cancel: pg_cancel_backend on the running backend, sent
		// over a side connection although the pool is exhausted.
		code, body = post("/api/query/cancel", map[string]any{"connectionId": "e2e", "requestId": reqID})
		if code != http.StatusOK || body["state"] != "sent" || body["method"] != "pg_cancel_backend" {
			t.Fatalf("cancel: %d %v", code, body)
		}
		res := await(ch, 10*time.Second)
		if res.elapsed > 20*time.Second {
			t.Errorf("cancelled statement took %s", res.elapsed)
		}
		if res.code != http.StatusOK || res.body["canceled"] != true || res.body["sqlState"] != "57014" ||
			res.body["requestId"] != reqID || res.body["connectionReused"] != true {
			t.Fatalf("cancelled query response: %d %v", res.code, res.body)
		}
		if msg, _ := res.body["error"].(string); !strings.Contains(msg, "user request") {
			t.Errorf("expected PostgreSQL's user-request cancel message, got %q", msg)
		}

		// The same backend survived (cancelled, not terminated) and serves
		// the next request.
		waitIdle(pid)
		if got := backendPID(); got != pid {
			t.Fatalf("next request ran on backend %d; the cancelled backend %d was not reused", got, pid)
		}
		if memoCount() != 3 {
			t.Fatal("the queued INSERT ran although it was cancelled before dispatch")
		}
		code, body = post("/api/query/cancel", map[string]any{"connectionId": "e2e", "requestId": reqID})
		if code != http.StatusNotFound || body["state"] != "not-running" {
			t.Errorf("cancel after completion: %d %v", code, body)
		}
	})

	t.Run("client disconnect cancels the statement server-side", func(t *testing.T) {
		pid := backendPID()
		const marker = "s04_disconnect_marker"
		ctx, abort := context.WithCancel(context.Background())
		ch := startAsync(ctx, "/api/query", map[string]any{
			"connectionId": "e2e", "requestId": "s04-disconnect-0001",
			"sql": "SELECT pg_sleep(60) /* " + marker + " */",
		})
		if got := waitActive(marker); got != pid {
			t.Fatalf("statement on backend %d, expected %d", got, pid)
		}
		abort()
		<-ch
		waitIdle(pid)
		if got := backendPID(); got != pid {
			t.Fatalf("after a disconnect-cancel the next request ran on %d, not %d", got, pid)
		}
	})

	t.Run("EXPLAIN plans without executing mutations", func(t *testing.T) {
		seq := seqState()
		for _, sql := range []string{
			"DELETE FROM memo",
			"UPDATE memo SET body = 'z'",
			"INSERT INTO memo VALUES (nextval('memo_seq'), 'x');",
			"WITH d AS (DELETE FROM memo RETURNING id) SELECT count(*) FROM d",
		} {
			code, body := post("/api/query/explain", map[string]any{"connectionId": "e2e", "sql": sql})
			if code != http.StatusOK || body["executed"] != false || body["analyze"] != false || body["committed"] != false {
				t.Fatalf("explain %q: %d %v", sql, code, body)
			}
			plan, ok := body["plan"].([]any)
			if !ok || len(plan) != 1 {
				t.Fatalf("explain %q: plan is not PostgreSQL's FORMAT JSON document: %v", sql, body["plan"])
			}
			root := plan[0].(map[string]any)["Plan"].(map[string]any)
			if root["Node Type"] == nil || root["Total Cost"] == nil {
				t.Errorf("explain %q: plan root lacks node type/cost: %v", sql, root)
			}
			if _, has := root["Actual Rows"]; has {
				t.Errorf("explain %q: non-analyze plan carries actual rows (it executed)", sql)
			}
			if memoCount() != 3 || seqState() != seq {
				t.Fatalf("plain EXPLAIN of %q changed data (count %d, seq %s→%s)", sql, memoCount(), seq, seqState())
			}
		}
		// CREATE TABLE AS under plain EXPLAIN creates nothing.
		code, body := post("/api/query/explain", map[string]any{"connectionId": "e2e", "sql": "CREATE TABLE s04_ctas AS SELECT * FROM memo"})
		if code != http.StatusOK {
			t.Fatalf("explain CTAS: %d %v", code, body)
		}
		var exists bool
		oracle(`SELECT to_regclass('s04_ctas') IS NOT NULL`, &exists)
		if exists {
			t.Fatal("plain EXPLAIN of CREATE TABLE AS created the table")
		}
		// Text after the statement cannot run outside the EXPLAIN.
		code, body = post("/api/query/explain", map[string]any{"connectionId": "e2e", "sql": "SELECT 1; DELETE FROM memo"})
		if code != http.StatusUnprocessableEntity || body["state"] != "sql-error" {
			t.Errorf("multi-statement explain: %d %v", code, body)
		}
		if memoCount() != 3 {
			t.Fatal("a smuggled second statement executed")
		}
	})

	t.Run("EXPLAIN ANALYZE executes, refuses writes by default, rolls back allowed writes", func(t *testing.T) {
		code, body := post("/api/query/explain", map[string]any{
			"connectionId": "e2e", "analyze": true,
			"sql": "SELECT * FROM memo WHERE id = $1", "params": []any{"2"},
		})
		if code != http.StatusOK || body["executed"] != true || body["readOnly"] != true || body["committed"] != false {
			t.Fatalf("analyze select: %d %v", code, body)
		}
		doc := body["plan"].([]any)[0].(map[string]any)
		root := doc["Plan"].(map[string]any)
		if !jsonNumberIs(root["Actual Rows"], 1) || doc["Execution Time"] == nil {
			t.Errorf("analyze plan lacks actual execution stats: %v", doc)
		}

		seq := seqState()
		for _, c := range []struct {
			sql      string
			sqlState string
		}{
			{"DELETE FROM memo", ""}, // refused by keyword before anything is sent
			{"CREATE TABLE s04_ctas AS SELECT * FROM memo", ""},
			{"WITH d AS (DELETE FROM memo RETURNING id) SELECT count(*) FROM d", "25006"}, // read-only transaction
			{"SELECT nextval('memo_seq')", "25006"},
		} {
			code, body := post("/api/query/explain", map[string]any{"connectionId": "e2e", "analyze": true, "sql": c.sql})
			if code != http.StatusUnprocessableEntity || body["state"] != "write-blocked" || body["executed"] != false {
				t.Errorf("analyze %q without allowWrites: %d %v", c.sql, code, body)
			}
			if c.sqlState != "" && body["sqlState"] != c.sqlState {
				t.Errorf("analyze %q: sqlState %v, want %s", c.sql, body["sqlState"], c.sqlState)
			}
			if memoCount() != 3 || seqState() != seq {
				t.Fatalf("analyze %q without allowWrites changed data", c.sql)
			}
		}

		code, body = post("/api/query/explain", map[string]any{
			"connectionId": "e2e", "analyze": true, "allowWrites": true, "sql": "DELETE FROM memo WHERE id <= 2",
		})
		if code != http.StatusOK || body["executed"] != true || body["writesAllowed"] != true || body["committed"] != false {
			t.Fatalf("analyze with allowWrites: %d %v", code, body)
		}
		root = body["plan"].([]any)[0].(map[string]any)["Plan"].(map[string]any)
		child := root["Plans"].([]any)[0].(map[string]any)
		if !jsonNumberIs(child["Actual Rows"], 2) {
			t.Errorf("allowWrites analyze did not execute the delete scan: %v", child)
		}
		if memoCount() != 3 {
			t.Fatal("EXPLAIN ANALYZE with allowWrites committed its writes; they must be rolled back")
		}
		code, body = post("/api/query/explain", map[string]any{
			"connectionId": "e2e", "analyze": true, "allowWrites": true, "sql": "CREATE TABLE s04_ctas AS SELECT * FROM memo",
		})
		if code != http.StatusOK {
			t.Fatalf("analyze CTAS with allowWrites: %d %v", code, body)
		}
		var exists bool
		oracle(`SELECT to_regclass('s04_ctas') IS NOT NULL`, &exists)
		if exists {
			t.Fatal("EXPLAIN ANALYZE CREATE TABLE AS with allowWrites left the table behind")
		}
	})

	t.Run("EXPLAIN ANALYZE is cancellable and its connection reused", func(t *testing.T) {
		pid := backendPID()
		const marker = "s04_explain_cancel_marker"
		ch := startAsync(context.Background(), "/api/query/explain", map[string]any{
			"connectionId": "e2e", "requestId": "s04-explain-cancel-1", "analyze": true,
			"sql": "SELECT pg_sleep(60) /* " + marker + " */",
		})
		waitActive(marker)
		code, body := post("/api/query/cancel", map[string]any{"connectionId": "e2e", "requestId": "s04-explain-cancel-1"})
		if code != http.StatusOK || body["state"] != "sent" {
			t.Fatalf("cancel explain: %d %v", code, body)
		}
		res := await(ch, 10*time.Second)
		if res.code != http.StatusUnprocessableEntity || res.body["state"] != "canceled" || res.body["connectionReused"] != true {
			t.Fatalf("cancelled explain: %d %v", res.code, res.body)
		}
		waitIdle(pid)
		if got := backendPID(); got != pid {
			t.Fatalf("after the cancelled EXPLAIN ANALYZE the next request ran on %d, not %d", got, pid)
		}
	})

	t.Run("saved queries keep working", func(t *testing.T) {
		code, body := post("/api/saved-queries", map[string]any{"name": "by id", "sql": "SELECT * FROM memo WHERE id = $1"})
		if code != http.StatusOK || body["id"] == nil {
			t.Fatalf("save: %d %v", code, body)
		}
		id := body["id"].(string)
		req, _ := http.NewRequest(http.MethodGet, baseURL+"/api/saved-queries", nil)
		res, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var list []SavedQuery
		json.NewDecoder(res.Body).Decode(&list) //nolint
		res.Body.Close()
		if len(list) != 1 || list[0].SQL != "SELECT * FROM memo WHERE id = $1" {
			t.Errorf("saved list: %+v", list)
		}
		code, _ = doCtx(context.Background(), http.MethodDelete, "/api/saved-queries/"+id, token, origin, "")
		if code != http.StatusNoContent {
			t.Errorf("delete saved: %d", code)
		}
	})
}

// jsonNumberIs compares a decoded JSON number by value (PostgreSQL 18 prints
// EXPLAIN ANALYZE row counts with decimals, 17 as integers).
func jsonNumberIs(v any, want float64) bool {
	n, ok := v.(json.Number)
	if !ok {
		return false
	}
	f, err := n.Float64()
	return err == nil && f == want
}
