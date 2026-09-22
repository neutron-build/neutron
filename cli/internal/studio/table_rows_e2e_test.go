package studio

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestStudioRowSafetyE2E exercises the V06 containment contract through REAL
// handler invocations (direct HTTP requests, no UI cooperation) against a
// REAL disposable Postgres database, with the table state verified through
// independently written SQL after every request.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server, or when NEUTRON_LIVE_REQUIRED=1 that missing URL is a failure
// instead of a skip. The test creates one uniquely-named database
// (neutron_orm_b04_*), seeds it, and drops it after terminating its
// connections.
func TestStudioRowSafetyE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio row-safety E2E skipped (set it to a disposable Postgres URL to run)")
	}

	dbName := fmt.Sprintf("neutron_orm_b04_%d_%d", os.Getpid(), time.Now().Unix())
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
		// Ownership verification: only ever drop the uniquely-named database this test created.
		if !strings.HasPrefix(dbName, "neutron_orm_b04_") {
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
	seed := []string{
		`CREATE TABLE docs (tenant_id int NOT NULL, id int NOT NULL, payload text NOT NULL, PRIMARY KEY (tenant_id, id))`,
		`INSERT INTO docs VALUES (1,1,'alpha'),(1,2,'beta'),(2,1,'gamma')`,
		`CREATE TABLE docs2 (tenant_id int NOT NULL, id int NOT NULL, payload text NOT NULL, PRIMARY KEY (tenant_id, id))`,
		`INSERT INTO docs2 VALUES (1,1,'d1'),(1,2,'d2'),(2,1,'d3')`,
		`CREATE TABLE memo (id int PRIMARY KEY, body text)`,
		`INSERT INTO memo VALUES (1,'original'),(2,'untouched')`,
		`CREATE TABLE keyless (tag text NOT NULL, note text NOT NULL)`,
		`INSERT INTO keyless VALUES ('a','keepme')`,
		`CREATE TABLE ident (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, note text NOT NULL)`,
		`INSERT INTO ident (note) VALUES ('gen-row')`,
	}
	for _, stmt := range seed {
		if err := fixture.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	s := &Server{clients: map[string]*db.Client{"e2e": fixture}}

	post := func(handler http.HandlerFunc, payload string) (int, map[string]any) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/table", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler(rec, req)
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode response %q: %v", rec.Body.String(), err)
		}
		return rec.Code, body
	}

	// Independent oracle helpers: read table state with hand-written SQL, not
	// the introspection or mutation helpers under test.
	docsState := func() []docRow {
		rows, err := fixture.Query(context.Background(), `SELECT tenant_id, id, payload FROM docs ORDER BY tenant_id, id`)
		if err != nil {
			t.Fatalf("oracle select docs: %v", err)
		}
		defer rows.Close()
		var out []docRow
		for rows.Next() {
			var d docRow
			if err := rows.Scan(&d.TenantID, &d.ID, &d.Payload); err != nil {
				t.Fatalf("oracle scan docs: %v", err)
			}
			out = append(out, d)
		}
		return out
	}
	docs2Count := func() int {
		var n int
		if err := fixture.QueryRow(context.Background(), `SELECT count(*) FROM docs2`).Scan(&n); err != nil {
			t.Fatalf("oracle count docs2: %v", err)
		}
		return n
	}
	memoBody := func(id int) (isNull bool, text string) {
		if err := fixture.QueryRow(context.Background(), `SELECT body IS NULL, COALESCE(body,'<null>') FROM memo WHERE id = $1`, id).Scan(&isNull, &text); err != nil {
			t.Fatalf("oracle memo %d: %v", id, err)
		}
		return isNull, text
	}
	keylessNote := func() string {
		var note string
		if err := fixture.QueryRow(context.Background(), `SELECT note FROM keyless WHERE tag = 'a'`).Scan(&note); err != nil {
			t.Fatalf("oracle keyless: %v", err)
		}
		return note
	}

	t.Run("composite-key update by one component is rejected and table unchanged", func(t *testing.T) {
		code, body := post(s.handleTableRowUpdate, `{
			"connectionId":"e2e","schema":"public","table":"docs",
			"pkColumn":"tenant_id","pkValue":1,"column":"payload","value":"CLOBBERED"}`)
		if code != http.StatusOK || body["rowsAffected"] != nil || body["error"] == nil {
			t.Fatalf("expected domain rejection, got %d %v", code, body)
		}
		if msg := body["error"].(string); !strings.Contains(msg, "composite primary key (tenant_id, id)") {
			t.Errorf("rejection should name the composite key: %q", msg)
		}
		want := []docRow{{1, 1, "alpha"}, {1, 2, "beta"}, {2, 1, "gamma"}}
		if got := docsState(); !equalDocs(got, want) {
			t.Errorf("docs changed after rejected update: %v", got)
		}
	})

	t.Run("composite-key delete by one component is rejected and table unchanged", func(t *testing.T) {
		code, body := post(s.handleTableRowDelete, `{
			"connectionId":"e2e","schema":"public","table":"docs2",
			"pkColumn":"tenant_id","pkValue":1}`)
		if code != http.StatusOK || body["rowsAffected"] != nil || body["error"] == nil {
			t.Fatalf("expected domain rejection, got %d %v", code, body)
		}
		if n := docs2Count(); n != 3 {
			t.Errorf("docs2 row count = %d after rejected delete, want 3", n)
		}
	})

	t.Run("forged pkColumn on single-key table is rejected before SQL", func(t *testing.T) {
		code, body := post(s.handleTableRowUpdate, `{
			"connectionId":"e2e","schema":"public","table":"memo",
			"pkColumn":"body","pkValue":"original","column":"body","value":"FORGED"}`)
		if code != http.StatusOK || body["rowsAffected"] != nil || body["error"] == nil {
			t.Fatalf("expected domain rejection, got %d %v", code, body)
		}
		if msg := body["error"].(string); !strings.Contains(msg, `pkColumn "body" is not the primary key`) {
			t.Errorf("rejection should name the forged key: %q", msg)
		}
		isNull, text := memoBody(1)
		if isNull || text != "original" {
			t.Errorf("memo row changed by forged request: null=%v text=%q", isNull, text)
		}
	})

	t.Run("forged pkColumn delete is rejected before SQL", func(t *testing.T) {
		code, body := post(s.handleTableRowDelete, `{
			"connectionId":"e2e","schema":"public","table":"memo",
			"pkColumn":"body","pkValue":"original"}`)
		if code != http.StatusOK || body["rowsAffected"] != nil || body["error"] == nil {
			t.Fatalf("expected domain rejection, got %d %v", code, body)
		}
		var n int
		if err := fixture.QueryRow(context.Background(), `SELECT count(*) FROM memo`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Errorf("memo rows = %d after rejected delete, want 2", n)
		}
	})

	t.Run("no-key table is read-only", func(t *testing.T) {
		code, body := post(s.handleTableRowUpdate, `{
			"connectionId":"e2e","schema":"public","table":"keyless",
			"pkColumn":"tag","pkValue":"a","column":"note","value":"x"}`)
		if code != http.StatusOK || body["rowsAffected"] != nil || body["error"] == nil {
			t.Fatalf("expected domain rejection, got %d %v", code, body)
		}
		if msg := body["error"].(string); !strings.Contains(msg, "has no primary key") {
			t.Errorf("rejection should explain missing key: %q", msg)
		}
		if note := keylessNote(); note != "keepme" {
			t.Errorf("keyless row changed after rejection: %q", note)
		}
	})

	t.Run("single-column PK update affects exactly one row", func(t *testing.T) {
		code, body := post(s.handleTableRowUpdate, `{
			"connectionId":"e2e","schema":"public","table":"memo",
			"pkColumn":"id","pkValue":1,"column":"body","value":"renamed"}`)
		if code != http.StatusOK || body["rowsAffected"] != float64(1) {
			t.Fatalf("expected success with rowsAffected 1, got %d %v", code, body)
		}
		isNull, text := memoBody(1)
		if isNull || text != "renamed" {
			t.Errorf("memo(1) = null:%v %q, want 'renamed'", isNull, text)
		}
		_, other := memoBody(2)
		if other != "untouched" {
			t.Errorf("memo(2) = %q, want 'untouched' (unrelated row changed)", other)
		}
	})

	t.Run("single-column PK delete affects exactly one row", func(t *testing.T) {
		code, body := post(s.handleTableRowDelete, `{
			"connectionId":"e2e","schema":"public","table":"memo",
			"pkColumn":"id","pkValue":2}`)
		if code != http.StatusOK || body["rowsAffected"] != float64(1) {
			t.Fatalf("expected success with rowsAffected 1, got %d %v", code, body)
		}
		var n int
		if err := fixture.QueryRow(context.Background(), `SELECT count(*) FROM memo`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("memo rows = %d after delete, want 1", n)
		}
	})

	t.Run("stale zero-row update reports an error, never success", func(t *testing.T) {
		code, body := post(s.handleTableRowUpdate, `{
			"connectionId":"e2e","schema":"public","table":"memo",
			"pkColumn":"id","pkValue":999,"column":"body","value":"ghost"}`)
		if code != http.StatusOK || body["rowsAffected"] != nil || body["error"] == nil {
			t.Fatalf("expected stale rejection, got %d %v", code, body)
		}
		if msg := body["error"].(string); !strings.Contains(msg, "stale") {
			t.Errorf("stale rejection should say stale/missing: %q", msg)
		}
		var n int
		if err := fixture.QueryRow(context.Background(), `SELECT count(*) FROM memo`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("memo rows = %d after stale update, want 1", n)
		}
	})

	t.Run("stale zero-row delete reports an error, never success", func(t *testing.T) {
		code, body := post(s.handleTableRowDelete, `{
			"connectionId":"e2e","schema":"public","table":"memo",
			"pkColumn":"id","pkValue":999}`)
		if code != http.StatusOK || body["rowsAffected"] != nil || body["error"] == nil {
			t.Fatalf("expected stale rejection, got %d %v", code, body)
		}
	})

	t.Run("isNull writes SQL NULL distinctly from empty string", func(t *testing.T) {
		code, body := post(s.handleTableRowUpdate, `{
			"connectionId":"e2e","schema":"public","table":"memo",
			"pkColumn":"id","pkValue":1,"column":"body","isNull":true}`)
		if code != http.StatusOK || body["rowsAffected"] != float64(1) {
			t.Fatalf("isNull update failed: %d %v", code, body)
		}
		if isNull, _ := memoBody(1); !isNull {
			t.Errorf("body should be SQL NULL after isNull update")
		}

		code, body = post(s.handleTableRowUpdate, `{
			"connectionId":"e2e","schema":"public","table":"memo",
			"pkColumn":"id","pkValue":1,"column":"body","value":""}`)
		if code != http.StatusOK || body["rowsAffected"] != float64(1) {
			t.Fatalf("empty-string update failed: %d %v", code, body)
		}
		isNull, text := memoBody(1)
		if isNull {
			t.Errorf("body must NOT be NULL after empty-string update (conflation)")
		}
		if text != "" {
			t.Errorf("body should be empty string, got %q", text)
		}
		var length int
		if err := fixture.QueryRow(context.Background(), `SELECT length(body) FROM memo WHERE id = 1`).Scan(&length); err != nil {
			t.Fatal(err)
		}
		if length != 0 {
			t.Errorf("length(body) = %d, want 0", length)
		}
	})

	t.Run("generated identity key column is read-only", func(t *testing.T) {
		code, body := post(s.handleTableRowUpdate, `{
			"connectionId":"e2e","schema":"public","table":"ident",
			"pkColumn":"id","pkValue":1,"column":"id","value":42}`)
		if code != http.StatusOK || body["rowsAffected"] != nil || body["error"] == nil {
			t.Fatalf("expected identity rejection, got %d %v", code, body)
		}
		if msg := body["error"].(string); !strings.Contains(msg, "read-only") {
			t.Errorf("identity rejection should say read-only: %q", msg)
		}
		var id int64
		if err := fixture.QueryRow(context.Background(), `SELECT id FROM ident LIMIT 1`).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if id != 1 {
			t.Errorf("ident.id = %d after rejected identity edit, want 1", id)
		}
	})

	t.Run("identity table non-key column edits still work", func(t *testing.T) {
		code, body := post(s.handleTableRowUpdate, `{
			"connectionId":"e2e","schema":"public","table":"ident",
			"pkColumn":"id","pkValue":1,"column":"note","value":"edited"}`)
		if code != http.StatusOK || body["rowsAffected"] != float64(1) {
			t.Fatalf("identity-table note edit failed: %d %v", code, body)
		}
		var note string
		if err := fixture.QueryRow(context.Background(), `SELECT note FROM ident LIMIT 1`).Scan(&note); err != nil {
			t.Fatal(err)
		}
		if note != "edited" {
			t.Errorf("ident.note = %q, want 'edited'", note)
		}
	})

	t.Run("missing table is rejected", func(t *testing.T) {
		code, body := post(s.handleTableRowUpdate, `{
			"connectionId":"e2e","schema":"public","table":"nonexistent",
			"pkColumn":"id","pkValue":1,"column":"x","value":"v"}`)
		if code != http.StatusOK || body["rowsAffected"] != nil || body["error"] == nil {
			t.Fatalf("expected rejection, got %d %v", code, body)
		}
	})

	t.Run("unknown target column is rejected", func(t *testing.T) {
		code, body := post(s.handleTableRowUpdate, `{
			"connectionId":"e2e","schema":"public","table":"memo",
			"pkColumn":"id","pkValue":1,"column":"no_such_col","value":"v"}`)
		if code != http.StatusOK || body["rowsAffected"] != nil || body["error"] == nil {
			t.Fatalf("expected rejection, got %d %v", code, body)
		}
		if msg := body["error"].(string); !strings.Contains(msg, "does not exist") {
			t.Errorf("unknown column rejection should say does not exist: %q", msg)
		}
	})
}

// docRow mirrors one row of the docs fixture for independent state checks.
type docRow struct {
	TenantID int
	ID       int
	Payload  string
}

func equalDocs(got, want []docRow) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func deriveStudioDatabaseURL(t *testing.T, base, dbName string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse NEUTRON_E2E_DATABASE_URL: %v", err)
	}
	u.Path = "/" + dbName
	return u.String()
}
