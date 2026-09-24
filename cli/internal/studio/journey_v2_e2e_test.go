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
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestStudioUserJourneyV2E2E walks one continuous Studio session — the exact
// HTTP conversation the SPA performs, through the REAL route table and
// corsMiddleware (host allowlist, exact-origin CORS, central session-token
// gate) against a REAL disposable Postgres database (V15 journey scope owned
// by S01: create/edit/delete with NULL/empty/DEFAULT distinction and the
// stale-row conflict, including recovery after a lost window).
//
// The connection is hand-registered on the Server instead of created through
// POST /api/connections, because the connection store persists to the user's
// config directory and a test must not mutate real user state. Everything
// else crosses the real HTTP boundary.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server; NEUTRON_LIVE_REQUIRED=1 turns a missing URL into a failure. Uses
// one uniquely-named s01_* database, dropped afterwards.
func TestStudioUserJourneyV2E2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio journey e2e skipped")
	}

	dbName := fmt.Sprintf("s01_%d_%d", os.Getpid(), time.Now().Unix())
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
		if !strings.HasPrefix(dbName, "s01_") {
			t.Errorf("refusing to drop unexpected database %q", dbName)
			return
		}
		if err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})

	fixture, err := db.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	defer fixture.Close()
	for _, stmt := range []string{
		`CREATE TABLE journey (id int PRIMARY KEY, body text NOT NULL, note text DEFAULT 'seed-default')`,
	} {
		if err := fixture.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Bind the real route table + middleware to a loopback listener whose
	// port the Server knows, so Host and Origin checks behave exactly as in
	// production.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	s := &Server{port: port, sessionToken: "s01-journey-token", clients: map[string]*db.Client{"e2e": fixture}}
	mux, err := s.routes()
	if err != nil {
		t.Fatalf("routes: %v", err)
	}
	ts := httptest.NewUnstartedServer(s.corsMiddleware(mux))
	ts.Listener = ln
	ts.Start()
	defer ts.Close()
	baseURL := ts.URL // http://127.0.0.1:<port>
	origin := baseURL

	client := &http.Client{Timeout: 10 * time.Second}

	// do performs one round-trip with browser-like headers. Token and Origin
	// are per-call because the journey also verifies refusals.
	do := func(method, path, token, originHeader string, body string) (int, map[string]any, string) {
		t.Helper()
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, baseURL+path, rd)
		if err != nil {
			t.Fatalf("request %s %s: %v", method, path, err)
		}
		if originHeader != "" {
			req.Header.Set("Origin", originHeader)
		}
		if token != "" {
			req.Header.Set(sessionHeader, token)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatalf("do %s %s: %v", method, path, err)
		}
		defer res.Body.Close()
		raw, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("read %s %s: %v", method, path, err)
		}
		var parsed map[string]any
		if len(raw) > 0 && strings.Contains(res.Header.Get("Content-Type"), "json") {
			dec := json.NewDecoder(strings.NewReader(string(raw)))
			dec.UseNumber()
			if err := dec.Decode(&parsed); err != nil {
				t.Fatalf("decode %s %s body %q: %v", method, path, raw, err)
			}
		}
		return res.StatusCode, parsed, string(raw)
	}

	// oracle verifies state with SQL written independently of the handler
	// paths (the fixture connection, not the Studio API).
	oracle := func(query string, dest ...any) {
		t.Helper()
		if err := fixture.QueryRow(context.Background(), query).Scan(dest...); err != nil {
			t.Fatalf("oracle %q: %v", query, err)
		}
	}

	// identity captures what a read hands the SPA for one row.
	type identity struct {
		binding string
		version string
	}

	var token string
	var binding string
	var rowsByID = map[int]identity{}

	// postMutation sends a v2 mutation the way lib/api.ts does (session
	// header + binding included).
	postMutation := func(path, payload string) (int, map[string]any) {
		t.Helper()
		var generic map[string]any
		dec := json.NewDecoder(strings.NewReader(payload))
		dec.UseNumber()
		if err := dec.Decode(&generic); err != nil {
			t.Fatalf("payload %q: %v", payload, err)
		}
		if _, has := generic["binding"]; !has {
			generic["binding"] = binding
			payload = mustJSON(generic)
		}
		code, body, _ := do(http.MethodPost, path, token, origin, payload)
		return code, body
	}

	t.Run("boot: SPA shell loads and yields the session token to the launch origin only", func(t *testing.T) {
		code, _, raw := do(http.MethodGet, "/", "", origin, "")
		if code != http.StatusOK {
			t.Fatalf("GET /: %d", code)
		}
		if !strings.Contains(raw, "<!DOCTYPE html>") && !strings.Contains(raw, "<html") {
			t.Fatalf("GET / did not serve the SPA shell: %q", raw[:min(len(raw), 120)])
		}

		code, body, _ := do(http.MethodGet, "/api/session", "", origin, "")
		if code != http.StatusOK || body["token"] != s.sessionToken {
			t.Fatalf("GET /api/session: %d %v", code, body)
		}
		token, _ = body["token"].(string)

		// A foreign origin is refused and never learns the token.
		code, body, _ = do(http.MethodGet, "/api/session", "", "http://evil.example", "")
		if code != http.StatusForbidden || body["auth"] != "origin" {
			t.Fatalf("GET /api/session from foreign origin: %d %v", code, body)
		}
	})

	t.Run("meta publishes the editable contract and relation binding", func(t *testing.T) {
		code, body, _ := do(http.MethodGet, "/api/table/v2/meta?connectionId=e2e&schema=public&table=journey", "", origin, "")
		if code != http.StatusOK {
			t.Fatalf("meta: %d %v", code, body)
		}
		if body["readOnly"] != false || body["versioned"] != true {
			t.Fatalf("meta state: %v", body)
		}
		b, _ := body["binding"].(string)
		if b == "" {
			t.Fatalf("meta carried no binding: %v", body)
		}
		binding = b
		cols := map[string]map[string]any{}
		for _, c := range body["columns"].([]any) {
			m := c.(map[string]any)
			cols[m["name"].(string)] = m
		}
		if cols["id"]["editable"] != false || cols["id"]["isKey"] != true {
			t.Fatalf("id column must be an uneditable key: %v", cols["id"])
		}
		if cols["body"]["editable"] != true {
			t.Fatalf("body column must be editable: %v", cols["body"])
		}
		if cols["note"]["hasDefault"] != true || cols["note"]["nullable"] != true {
			t.Fatalf("note column must be nullable-with-default: %v", cols["note"])
		}
	})

	t.Run("create: NULL, empty string and DEFAULT are three distinct outcomes", func(t *testing.T) {
		// Explicit values.
		code, body := postMutation("/api/table/v2/insert", `{"connectionId":"e2e","schema":"public","table":"journey","values":{"id":1,"body":"one","note":"explicit"}}`)
		if code != http.StatusOK {
			t.Fatalf("insert 1: %d %v", code, body)
		}
		// SQL NULL for the nullable column (JSON null, not omitted).
		code, body = postMutation("/api/table/v2/insert", `{"connectionId":"e2e","schema":"public","table":"journey","values":{"id":2,"body":"two","note":null}}`)
		if code != http.StatusOK {
			t.Fatalf("insert 2 (NULL): %d %v", code, body)
		}
		// Empty string is a VALUE, not a NULL and not a DEFAULT.
		code, body = postMutation("/api/table/v2/insert", `{"connectionId":"e2e","schema":"public","table":"journey","values":{"id":3,"body":"","note":""}}`)
		if code != http.StatusOK {
			t.Fatalf("insert 3 (empty): %d %v", code, body)
		}
		// Omission requests the column DEFAULT.
		code, body = postMutation("/api/table/v2/insert", `{"connectionId":"e2e","schema":"public","table":"journey","values":{"id":4,"body":"four"}}`)
		if code != http.StatusOK {
			t.Fatalf("insert 4 (DEFAULT): %d %v", code, body)
		}
		if v, _ := body["version"].(string); v == "" {
			t.Fatalf("insert did not return the new row version: %v", body)
		}

		var n int
		oracle(`SELECT count(*) FROM journey`, &n)
		if n != 4 {
			t.Fatalf("journey has %d rows, want 4", n)
		}
		var note1, note3, note4 string
		var null2 bool
		oracle(`SELECT note FROM journey WHERE id = 1`, &note1)
		oracle(`SELECT note IS NULL FROM journey WHERE id = 2`, &null2)
		oracle(`SELECT note FROM journey WHERE id = 3`, &note3)
		oracle(`SELECT note FROM journey WHERE id = 4`, &note4)
		var body3 string
		oracle(`SELECT body FROM journey WHERE id = 3`, &body3)
		if note1 != "explicit" || !null2 || note3 != "" || body3 != "" || note4 != "seed-default" {
			t.Fatalf("NULL/empty/DEFAULT conflated: note1=%q null2=%v note3=%q body3=%q note4=%q",
				note1, null2, note3, body3, note4)
		}
	})

	t.Run("read returns versioned identities the SPA can address rows by", func(t *testing.T) {
		code, body, _ := do(http.MethodGet, "/api/table?connectionId=e2e&schema=public&table=journey", "", origin, "")
		if code != http.StatusOK {
			t.Fatalf("read: %d %v", code, body)
		}
		if b, _ := body["binding"].(string); b != binding {
			t.Fatalf("read binding %q != meta binding %q", b, binding)
		}
		cols := body["columns"].([]any)
		versions := body["versions"].([]any)
		rows := body["rows"].([]any)
		if len(rows) != 4 || len(versions) != 4 {
			t.Fatalf("read shape: %d rows, %d versions", len(rows), len(versions))
		}
		idIdx, noteIdx := -1, -1
		for i, c := range cols {
			switch c {
			case "id":
				idIdx = i
			case "note":
				noteIdx = i
			}
		}
		for i, r := range rows {
			row := r.([]any)
			idNum, err := row[idIdx].(json.Number).Int64()
			if err != nil {
				t.Fatalf("row %d id: %v", i, err)
			}
			v, _ := versions[i].(string)
			if v == "" {
				t.Fatalf("row %d has no version", i)
			}
			rowsByID[int(idNum)] = identity{binding: binding, version: v}
			_ = noteIdx
		}
	})

	t.Run("edit: NULL and empty string stay distinct through updates", func(t *testing.T) {
		id2 := rowsByID[2]
		code, body := postMutation("/api/table/v2/update", fmt.Sprintf(
			`{"connectionId":"e2e","schema":"public","table":"journey","binding":%q,"key":[{"column":"id","value":2}],"version":%q,"column":"note","isNull":true}`,
			id2.binding, id2.version))
		if code != http.StatusOK {
			t.Fatalf("update to NULL: %d %v", code, body)
		}
		var null bool
		oracle(`SELECT note IS NULL FROM journey WHERE id = 2`, &null)
		if !null {
			t.Fatal("note should be SQL NULL after isNull update")
		}
		newV, _ := body["version"].(string)
		if newV == "" || newV == id2.version {
			t.Fatalf("update must return the row's new version: %q -> %q", id2.version, newV)
		}
		rowsByID[2] = identity{binding: binding, version: newV}

		id3 := rowsByID[3]
		code, body = postMutation("/api/table/v2/update", fmt.Sprintf(
			`{"connectionId":"e2e","schema":"public","table":"journey","binding":%q,"key":[{"column":"id","value":3}],"version":%q,"column":"note","value":"written"}`,
			id3.binding, id3.version))
		if code != http.StatusOK {
			t.Fatalf("update to value: %d %v", code, body)
		}
		var note string
		oracle(`SELECT note FROM journey WHERE id = 3`, &note)
		if note != "written" {
			t.Fatalf("note = %q, want written", note)
		}
		if v, _ := body["version"].(string); v != "" {
			rowsByID[3] = identity{binding: binding, version: v}
		}
	})

	t.Run("lost window: external write turns the stale edit into an explicit conflict, re-read recovers", func(t *testing.T) {
		id1 := rowsByID[1]

		// Someone else writes the row out from under the SPA.
		if err := fixture.Exec(context.Background(), `UPDATE journey SET note = 'extern' WHERE id = 1`); err != nil {
			t.Fatalf("external write: %v", err)
		}
		var current string
		oracle(`SELECT xmin::text FROM journey WHERE id = 1`, &current)

		code, body := postMutation("/api/table/v2/update", fmt.Sprintf(
			`{"connectionId":"e2e","schema":"public","table":"journey","binding":%q,"key":[{"column":"id","value":1}],"version":%q,"column":"note","value":"stale-edit"}`,
			id1.binding, id1.version))
		if code != http.StatusConflict {
			t.Fatalf("stale edit must 409, got %d %v", code, body)
		}
		if body["state"] != "conflict" {
			t.Fatalf("stale edit state: %v", body)
		}
		if cv, _ := body["currentVersion"].(string); cv != current {
			t.Fatalf("conflict currentVersion %q != actual xmin %q", cv, current)
		}
		var note string
		oracle(`SELECT note FROM journey WHERE id = 1`, &note)
		if note != "extern" {
			t.Fatalf("stale edit overwrote the external write: note = %q", note)
		}

		// The SPA reloads the row and reapplies the edit successfully.
		_, fresh, _ := do(http.MethodGet, "/api/table?connectionId=e2e&schema=public&table=journey", "", origin, "")
		_ = fresh
		var reloaded string
		oracle(`SELECT xmin::text FROM journey WHERE id = 1`, &reloaded)
		code, body = postMutation("/api/table/v2/update", fmt.Sprintf(
			`{"connectionId":"e2e","schema":"public","table":"journey","binding":%q,"key":[{"column":"id","value":1}],"version":%q,"column":"note","value":"recovered"}`,
			id1.binding, reloaded))
		if code != http.StatusOK {
			t.Fatalf("recovered edit: %d %v", code, body)
		}
		oracle(`SELECT note FROM journey WHERE id = 1`, &note)
		if note != "recovered" {
			t.Fatalf("note = %q, want recovered", note)
		}
	})

	t.Run("self-conflict: the pre-edit version is refused after your own edit", func(t *testing.T) {
		id4 := rowsByID[4]
		code, body := postMutation("/api/table/v2/update", fmt.Sprintf(
			`{"connectionId":"e2e","schema":"public","table":"journey","binding":%q,"key":[{"column":"id","value":4}],"version":%q,"column":"note","value":"first"}`,
			id4.binding, id4.version))
		if code != http.StatusOK {
			t.Fatalf("first edit: %d %v", code, body)
		}
		firstV, _ := body["version"].(string)

		code, body = postMutation("/api/table/v2/update", fmt.Sprintf(
			`{"connectionId":"e2e","schema":"public","table":"journey","binding":%q,"key":[{"column":"id","value":4}],"version":%q,"column":"note","value":"second"}`,
			id4.binding, id4.version))
		if code != http.StatusConflict || body["state"] != "conflict" {
			t.Fatalf("self-conflict must 409 conflict, got %d %v", code, body)
		}
		if cv, _ := body["currentVersion"].(string); cv != firstV {
			t.Fatalf("self-conflict currentVersion %q != first-edit version %q", cv, firstV)
		}
		var note string
		oracle(`SELECT note FROM journey WHERE id = 4`, &note)
		if note != "first" {
			t.Fatalf("note = %q, want first", note)
		}
		rowsByID[4] = identity{binding: binding, version: firstV}
	})

	t.Run("delete through the identity, then the same identity is missing", func(t *testing.T) {
		id4 := rowsByID[4]
		code, body := postMutation("/api/table/v2/delete", fmt.Sprintf(
			`{"connectionId":"e2e","schema":"public","table":"journey","binding":%q,"key":[{"column":"id","value":4}],"version":%q}`,
			id4.binding, id4.version))
		if code != http.StatusOK {
			t.Fatalf("delete: %d %v", code, body)
		}
		var n int
		oracle(`SELECT count(*) FROM journey WHERE id = 4`, &n)
		if n != 0 {
			t.Fatal("row 4 still present after delete")
		}

		code, body = postMutation("/api/table/v2/delete", fmt.Sprintf(
			`{"connectionId":"e2e","schema":"public","table":"journey","binding":%q,"key":[{"column":"id","value":4}],"version":%q}`,
			id4.binding, id4.version))
		if code != http.StatusConflict || body["state"] != "missing" {
			t.Fatalf("second delete must be 409 missing, got %d %v", code, body)
		}
	})

	t.Run("the boundary refuses unauthenticated and wrong-origin mutations", func(t *testing.T) {
		payload := `{"connectionId":"e2e","schema":"public","table":"journey","values":{"id":99,"body":"nope"}}`

		// Right origin, no session token.
		code, body, _ := do(http.MethodPost, "/api/table/v2/insert", "", origin, payload)
		if code != http.StatusForbidden || body["auth"] != "session" {
			t.Fatalf("tokenless mutation: %d %v", code, body)
		}

		// Token, but a foreign origin (another localhost port is still foreign).
		code, body, _ = do(http.MethodPost, "/api/table/v2/insert", token, fmt.Sprintf("http://127.0.0.1:%d", port+1), payload)
		if code != http.StatusForbidden || body["auth"] != "origin" {
			t.Fatalf("wrong-origin mutation: %d %v", code, body)
		}

		var n int
		oracle(`SELECT count(*) FROM journey WHERE id = 99`, &n)
		if n != 0 {
			t.Fatal("refused mutation was applied")
		}
	})
}
