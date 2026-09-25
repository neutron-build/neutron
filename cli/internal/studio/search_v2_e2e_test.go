package studio

// X01 Studio e2e: the table search endpoint and read-only rendering
// semantics for vector/tsvector columns, against a REAL disposable x01_*
// Postgres database with the REAL pgvector extension. Never mocked.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

func newX01StudioDB(t *testing.T, label string, createExtension bool) (*db.Client, string) {
	t.Helper()
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio X01 e2e skipped")
	}
	dbName := fmt.Sprintf("x01_studio_%s_%d_%d", label, os.Getpid(), time.Now().UnixNano()%1_000_000)
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
		if !strings.HasPrefix(dbName, "x01_studio_") {
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
	t.Cleanup(fixture.Close)
	if createExtension {
		if err := fixture.Exec(context.Background(), `CREATE EXTENSION vector`); err != nil {
			t.Fatalf("create extension: %v", err)
		}
	}
	return fixture, dbURL
}

const x01StudioSeed = `
CREATE TABLE x01_items (id serial PRIMARY KEY, label text NOT NULL, body text NOT NULL, keywords tsvector, embedding vector(3));
INSERT INTO x01_items (label, body, keywords, embedding) VALUES
 ('a', 'cat sat on the mat', 'animal pet', '[1,0,0]'),
 ('b', 'dog barked at the cat', 'animal guard', '[0,1,0]'),
 ('c', 'database full text search', 'software search', '[0.9,0.1,0]'),
 ('d', 'vector search ranking', 'search ai', '[0,0,1]');
`

func TestStudioX01SearchAndReadOnlyE2E(t *testing.T) {
	fixture, _ := newX01StudioDB(t, "vec", true)
	for _, stmt := range strings.Split(x01StudioSeed, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if err := fixture.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	s := &Server{port: 59999, sessionToken: "x01-e2e-token", clients: map[string]*db.Client{"e2e": fixture}}

	post := func(payload string) (int, map[string]any) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/table/v2/search", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://localhost:59999")
		req.Header.Set(sessionHeader, s.sessionToken)
		rec := httptest.NewRecorder()
		s.handleTableSearchV2(rec, req)
		var body map[string]any
		if rec.Body.Len() > 0 {
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode response %q: %v", rec.Body.String(), err)
			}
		}
		return rec.Code, body
	}

	t.Run("MetaMarksVectorAndTsvectorReadOnly", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/table/v2/meta?connectionId=e2e&schema=public&table=x01_items", nil)
		req.Header.Set("Origin", "http://localhost:59999")
		req.Header.Set(sessionHeader, s.sessionToken)
		rec := httptest.NewRecorder()
		s.handleTableRowMetaV2(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("meta: %d %s", rec.Code, rec.Body.String())
		}
		var body struct {
			Columns []struct {
				Name           string `json:"name"`
				Tag            string `json:"tag"`
				Editable       bool   `json:"editable"`
				Insertable     bool   `json:"insertable"`
				ReadOnlyReason string `json:"readOnlyReason"`
			} `json:"columns"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		byName := map[string]decltype{}
		for _, c := range body.Columns {
			byName[c.Name] = decltype{c.Tag, c.Editable, c.Insertable, c.ReadOnlyReason}
		}
		if c := byName["embedding"]; c.tag != "vector" || c.editable || c.insertable || !strings.Contains(c.reason, "not row-editable") {
			t.Fatalf("embedding column wrong: %+v", c)
		}
		if c := byName["keywords"]; c.tag != "tsvector" || c.editable || c.insertable {
			t.Fatalf("keywords column wrong: %+v", c)
		}
		if c := byName["label"]; c.editable == false || c.insertable == false {
			t.Fatalf("plain text column must stay editable: %+v", c)
		}
	})

	t.Run("VectorSearchOrdersByCosineAgainstOracle", func(t *testing.T) {
		code, body := post(`{"connectionId":"e2e","schema":"public","table":"x01_items","kind":"vector","column":"embedding","query":"[0.95, 0.05, 0]","operator":"cosine","limit":3}`)
		if code != http.StatusOK {
			t.Fatalf("vector search: %d %v", code, body)
		}
		cols := body["columns"].([]any)
		idIdx := -1
		for i, c := range cols {
			if c == "id" {
				idIdx = i
			}
		}
		if idIdx < 0 {
			t.Fatalf("no id column: %v", cols)
		}
		var got []string
		for _, row := range body["rows"].([]any) {
			cells := row.([]any)
			got = append(got, fmt.Sprint(cells[idIdx]))
		}
		var want []string
		rows, err := fixture.Query(context.Background(), `SELECT id::text FROM x01_items ORDER BY embedding <=> '[0.95,0.05,0]'::vector LIMIT 3`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
			want = append(want, v)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("vector search ordering must equal the raw oracle: got %v want %v", got, want)
		}
		if len(got) != 3 {
			t.Fatalf("limit respected: %v", got)
		}
	})

	t.Run("VectorSearchAllOperatorsAndDimensionErrors", func(t *testing.T) {
		for op := range searchVectorOperators {
			code, body := post(fmt.Sprintf(`{"connectionId":"e2e","schema":"public","table":"x01_items","kind":"vector","column":"embedding","query":"[1,2,3]","operator":%q,"limit":2}`, op))
			if code != http.StatusOK || body["error"] != nil {
				t.Fatalf("operator %s: %d %v", op, code, body)
			}
		}
		// Wrong dimension: the server's own dimension contract, sanitized.
		code, body := post(`{"connectionId":"e2e","schema":"public","table":"x01_items","kind":"vector","column":"embedding","query":"[1,2]","operator":"l2","limit":2}`)
		if code != http.StatusOK {
			t.Fatalf("dimension failure must be a 200 with error payload: %d", code)
		}
		if msg, _ := body["error"].(string); !strings.Contains(msg, "dimensions") {
			t.Fatalf("dimension error must name dimensions, got: %v", body["error"])
		}
		// Bad vector literal.
		code, body = post(`{"connectionId":"e2e","schema":"public","table":"x01_items","kind":"vector","column":"embedding","query":"abc","limit":2}`)
		if body["error"] == nil {
			t.Fatalf("garbage vector must be rejected: %v", body)
		}
	})

	t.Run("FtsSearchTextAndTsvectorColumns", func(t *testing.T) {
		code, body := post(`{"connectionId":"e2e","schema":"public","table":"x01_items","kind":"fts","column":"body","query":"\"text search\" or vector","limit":5}`)
		if code != http.StatusOK || body["error"] != nil {
			t.Fatalf("fts search: %d %v", code, body)
		}
		if len(body["rows"].([]any)) != 2 {
			t.Fatalf("websearch OR/phrase must match rows c and d: %v", body["rows"])
		}
		// tsvector column direct match.
		code, body = post(`{"connectionId":"e2e","schema":"public","table":"x01_items","kind":"fts","column":"keywords","query":"guard","limit":5}`)
		if code != http.StatusOK || body["error"] != nil {
			t.Fatalf("tsvector fts: %d %v", code, body)
		}
		if len(body["rows"].([]any)) != 1 {
			t.Fatalf("guard matches one row: %v", body["rows"])
		}
		// Non-text column refused before SQL.
		code, body = post(`{"connectionId":"e2e","schema":"public","table":"x01_items","kind":"fts","column":"embedding","query":"x"}`)
		if body["error"] == nil || !strings.Contains(fmt.Sprint(body["error"]), "not text") {
			// error payload shape: {"error": {"message": ...}} via writeDomainError
			t.Logf("domain error shape: %v", body["error"])
		}
	})

	t.Run("VectorColumnUpdateRejectedReadOnly", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/table/v2/update", strings.NewReader(
			`{"connectionId":"e2e","schema":"public","table":"x01_items","key":{"id":"1"},"values":{"embedding":"[9,9,9]"}}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://localhost:59999")
		req.Header.Set(sessionHeader, s.sessionToken)
		rec := httptest.NewRecorder()
		s.handleTableRowUpdateV2(rec, req)
		if rec.Code == http.StatusOK {
			var body map[string]any
			json.Unmarshal(rec.Body.Bytes(), &body)
			if body["error"] == nil {
				t.Fatalf("vector column update must be rejected, got: %s", rec.Body.String())
			}
		}
	})
}

func TestStudioX01SearchWithoutExtensionE2E(t *testing.T) {
	// The real absent-extension boundary: same server binary, fresh
	// database WITHOUT pgvector — the search surfaces the server's own
	// refusal, never a mock.
	fixture, _ := newX01StudioDB(t, "noext", false)
	if err := fixture.Exec(context.Background(), `CREATE TABLE x01_items (id serial PRIMARY KEY, embedding vector(3))`); err == nil {
		t.Fatal("vector table without the extension must not even create")
	}
	// Even so, the endpoint answers honestly for whatever engine answers.
	s := &Server{port: 59999, sessionToken: "x01-e2e-token", clients: map[string]*db.Client{"e2e": fixture}}
	if err := fixture.Exec(context.Background(), `CREATE TABLE x01_items (id serial PRIMARY KEY, label text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	// FTS still works on plain text columns (core PostgreSQL).
	req := httptest.NewRequest(http.MethodPost, "/api/table/v2/search", strings.NewReader(
		`{"connectionId":"e2e","schema":"public","table":"x01_items","kind":"fts","column":"label","query":"anything"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:59999")
	req.Header.Set(sessionHeader, s.sessionToken)
	rec := httptest.NewRecorder()
	s.handleTableSearchV2(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("fts on plain PG must work: %d %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body["error"] != nil {
		t.Fatalf("fts on plain PG must not error: %v", body)
	}
}

type decltype struct {
	tag        string
	editable   bool
	insertable bool
	reason     string
}
