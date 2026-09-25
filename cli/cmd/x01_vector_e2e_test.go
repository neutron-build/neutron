package cmd

// X01 end-to-end coverage: vectors and search through the REAL CLI binary
// against disposable x01_* Postgres databases. The extension boundary is
// real: pgvector is available on the server (brew) but NOT installed in the
// fresh database, so the absent-extension leg exercises the genuine
// fail-closed gate, and `create extension vector` flips it to the real
// pgvector surface. Never mocked.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// A vector-bearing document: vector(3) column, hnsw (cosine opclass + with
// parameters), ivfflat (ip opclass), tsvector column with a gin index.
const x01VectorDoc = `{
	"version": 2, "dialect": "postgresql", "capabilities": ["pgvector"],
	"schemas": [{"name": "public"}],
	"tables": [
		{
			"identity": {"schema": "public", "name": "x01_e2e_docs"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true,
				 "default": {"kind": "sequence", "sequence": {"schema": "public", "name": "x01_e2e_docs_id_seq"}}},
				{"name": "body", "type": {"name": "text", "codec": "string"}, "notNull": true},
				{"name": "keywords", "type": {"name": "tsvector", "codec": "tsvector"}, "notNull": false},
				{"name": "embedding", "type": {"name": "vector", "codec": "vector", "params": {"dimensions": 3}}, "notNull": false}
			],
			"constraints": [
				{"name": "x01_e2e_docs_pkey", "type": "primary-key", "columns": ["id"]}
			],
			"indexes": [
				{"identity": {"schema": "public", "name": "x01_e2e_docs_emb_hnsw"}, "unique": false, "method": "hnsw",
				 "key": [{"column": "embedding", "opclass": "vector_cosine_ops"}],
				 "with": {"ef_construction": 64, "m": 16}},
				{"identity": {"schema": "public", "name": "x01_e2e_docs_emb_ivfflat"}, "unique": false, "method": "ivfflat",
				 "key": [{"column": "embedding", "opclass": "vector_ip_ops"}],
				 "with": {"lists": 10}},
				{"identity": {"schema": "public", "name": "x01_e2e_docs_kw_gin"}, "unique": false, "method": "gin",
				 "key": [{"column": "keywords"}]}
			]
		}
	],
	"enums": [], "views": [], "opaque": []
}`

func newX01CommandDB(t *testing.T, label string) (string, *db.Client) {
	t.Helper()
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; X01 e2e skipped (set it to a disposable Postgres URL to run)")
	}
	return newM02CommandDB(t, "x01_"+label)
}

func TestX01VectorPushPullE2E(t *testing.T) {
	dbURL, fixture := newX01CommandDB(t, "vec")
	bin := buildCLIBinary(t)
	work := t.TempDir()
	doc := filepath.Join(work, "x01.json")
	writeFile(t, doc, x01VectorDoc)

	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}
	queryStr := func(sql string) string {
		t.Helper()
		var v string
		if err := fixture.QueryRow(context.Background(), sql).Scan(&v); err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		return v
	}

	t.Run("AbsentExtensionFailsClosedBeforeAnyDDL", func(t *testing.T) {
		code, out := run("db", "push", "--schema", doc)
		if code == 0 {
			t.Fatalf("push into a database without pgvector must fail, got success:\n%s", out)
		}
		if !strings.Contains(out, "requires the pgvector extension") || !strings.Contains(out, "never skipped") {
			t.Fatalf("failure must name the extension and the never-skipped contract:\n%s", out)
		}
		if got := queryStr(`SELECT count(*)::text FROM pg_tables WHERE tablename='x01_e2e_docs'`); got != "0" {
			t.Fatalf("no table may exist after the refused push (a skipped column would leave a crippled table)")
		}
		// Dry-run reports the same refusal honestly.
		code, out = run("db", "push", "--schema", doc, "--dry-run")
		if code == 0 || !strings.Contains(out, "requires the pgvector extension") {
			t.Fatalf("dry run must surface the same precondition:\n%s", out)
		}
	})

	t.Run("WithExtensionPushConvergesAndIndexesCarrySemantics", func(t *testing.T) {
		if err := fixture.Exec(context.Background(), `CREATE EXTENSION vector`); err != nil {
			t.Fatalf("create extension: %v", err)
		}
		code, out := run("db", "push", "--schema", doc)
		if code != 0 {
			t.Fatalf("push with the real pgvector failed (%d):\n%s", code, out)
		}
		code, out = run("db", "push", "--schema", doc)
		if code != 0 || !strings.Contains(out, "in sync") {
			t.Fatalf("second push must converge (%d):\n%s", code, out)
		}
		hnsw := queryStr(`SELECT indexdef FROM pg_indexes WHERE indexname='x01_e2e_docs_emb_hnsw'`)
		for _, want := range []string{"USING hnsw", "embedding vector_cosine_ops", "m='16'", "ef_construction='64'"} {
			if !strings.Contains(hnsw, want) {
				t.Fatalf("hnsw indexdef missing %q: %s", want, hnsw)
			}
		}
		ivf := queryStr(`SELECT indexdef FROM pg_indexes WHERE indexname='x01_e2e_docs_emb_ivfflat'`)
		if !strings.Contains(ivf, "vector_ip_ops") || !strings.Contains(ivf, "lists='10'") {
			t.Fatalf("ivfflat indexdef wrong: %s", ivf)
		}
	})

	t.Run("WriteQueryInspectRoundTrip", func(t *testing.T) {
		// Write: one row with a vector and a tsvector, through real SQL.
		if err := fixture.Exec(context.Background(),
			`INSERT INTO x01_e2e_docs (body, keywords, embedding) VALUES ($1, $2, $3)`,
			"doc about cats", "animal pet", "[1,2,3]"); err != nil {
			t.Fatalf("insert: %v", err)
		}
		// Query: distance ordering + FTS match through the server.
		var id int
		if err := fixture.QueryRow(context.Background(),
			`SELECT id FROM x01_e2e_docs ORDER BY embedding <=> '[1,2,3]'::vector LIMIT 1`).Scan(&id); err != nil {
			t.Fatalf("distance query: %v", err)
		}
		if err := fixture.QueryRow(context.Background(),
			`SELECT id FROM x01_e2e_docs WHERE keywords @@ plainto_tsquery('english', 'pet')`).Scan(&id); err != nil {
			t.Fatalf("fts query: %v", err)
		}
		// Inspect: pull the live document; it must carry the vector column
		// with dimensions, the tsvector column, and both indexes with their
		// operator classes and access-method parameters.
		pulled := filepath.Join(work, "pulled.json")
		code, out := run("schema", "pull", "--out", pulled)
		if code != 0 {
			t.Fatalf("schema pull failed (%d):\n%s", code, out)
		}
		raw, err := os.ReadFile(pulled)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{
			`"name":"vector"`, `"dimensions":3`,
			`"name":"tsvector"`,
			`"method":"hnsw"`, `"opclass":"vector_cosine_ops"`,
			`"ef_construction":64`, `"m":16`,
			`"method":"ivfflat"`, `"opclass":"vector_ip_ops"`, `"lists":10`,
			`"method":"gin"`,
			`"capabilities":["pgvector"]`,
		} {
			if !strings.Contains(string(raw), want) {
				t.Fatalf("pulled document missing %q:\n%s", want, raw)
			}
		}
		// The pulled document must itself validate (round-trip legality).
		if _, err := db.ParseV2Document(raw); err != nil {
			t.Fatalf("pulled document must validate: %v", err)
		}

		// Restart invariance (V18): a second pull from a fresh CLI process
		// (new connection) canonicalizes identically.
		pulled2 := filepath.Join(work, "pulled2.json")
		code, out = run("schema", "pull", "--out", pulled2)
		if code != 0 {
			t.Fatalf("second pull failed (%d):\n%s", code, out)
		}
		d1, err := db.ParseV2Document(raw)
		if err != nil {
			t.Fatal(err)
		}
		raw2, err := os.ReadFile(pulled2)
		if err != nil {
			t.Fatal(err)
		}
		d2, err := db.ParseV2Document(raw2)
		if err != nil {
			t.Fatal(err)
		}
		if d1.SHA256Hex != d2.SHA256Hex {
			t.Fatalf("pulls across reconnects must canonicalize identically: %s vs %s", d1.SHA256Hex, d2.SHA256Hex)
		}

		// Convergence with the introspected world: pushing the ORIGINAL doc
		// after pull stays in sync (introspection agrees with the contract).
		code, out = run("db", "push", "--schema", doc)
		if code != 0 || !strings.Contains(out, "in sync") {
			t.Fatalf("push after pull must stay in sync (%d):\n%s", code, out)
		}
	})

	t.Run("DimensionFailureAtTheServer", func(t *testing.T) {
		// The server enforces the declared dimension: a wrong-dimension
		// insert is refused by pgvector itself (22000) — the dimension is a
		// schema contract enforced at both ends.
		if err := fixture.Exec(context.Background(),
			`INSERT INTO x01_e2e_docs (body, embedding) VALUES ($1, $2)`, "bad", "[1,2]"); err == nil {
			t.Fatal("wrong-dimension insert must fail at the server")
		} else if !strings.Contains(err.Error(), "dimensions") {
			t.Fatalf("expected a dimension error, got: %v", err)
		}
	})
}

func TestX01MigrationFailsNotSkipsE2E(t *testing.T) {
	dbURL, fixture := newX01CommandDB(t, "mig")
	bin := buildCLIBinary(t)
	work := t.TempDir()
	doc := filepath.Join(work, "x01.json")
	writeFile(t, doc, x01VectorDoc)
	mig := filepath.Join(work, "mig")
	if err := os.MkdirAll(mig, 0o755); err != nil {
		t.Fatal(err)
	}

	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	t.Run("GenerateOfflineThenApplyRefusedWithoutExtension", func(t *testing.T) {
		code, out := run("migrate", "generate", "--schema", doc, "--dir", mig, "--name", "x01_vector", "--mode", "snapshot")
		if code != 0 {
			t.Fatalf("generate must succeed offline (%d):\n%s", code, out)
		}
		up := readFile(t, filepath.Join(mig, "001_x01_vector.up.sql"))
		// The migration CARRIES the vector objects (never skipped, never
		// commented out) - the old NUCLEUS-ONLY skip is withdrawn (X01).
		for _, want := range []string{"vector(3)", "using hnsw", "vector_cosine_ops", "tsvector"} {
			if !strings.Contains(up, want) {
				t.Fatalf("generated migration missing %q (skipped?):\n%s", want, up)
			}
		}
		// Apply against the extension-less database: refused BEFORE any
		// statement, with the remediation.
		code, out = run("migrate", "apply", "--dir", mig)
		if code == 0 {
			t.Fatalf("apply without pgvector must fail, got:\n%s", out)
		}
		if !strings.Contains(out, "require the pgvector extension") || !strings.Contains(out, "create extension vector") {
			t.Fatalf("refusal must name the extension and remediation:\n%s", out)
		}
		// Nothing applied, nothing created - the history is untouched.
		var n string
		if err := fixture.QueryRow(context.Background(), `SELECT count(*)::text FROM pg_tables WHERE tablename='x01_e2e_docs'`).Scan(&n); err != nil {
			t.Fatalf("count tables: %v", err)
		}
		if n != "0" {
			t.Fatalf("no table may exist after the refused apply (got %s)", n)
		}
	})

	t.Run("AfterCreateExtensionApplySucceedsAndRecordsHistory", func(t *testing.T) {
		if err := fixture.Exec(context.Background(), `CREATE EXTENSION vector`); err != nil {
			t.Fatalf("create extension: %v", err)
		}
		code, out := run("migrate", "apply", "--dir", mig)
		if code != 0 {
			t.Fatalf("apply with pgvector failed (%d):\n%s", code, out)
		}
		var applied string
		if err := fixture.QueryRow(context.Background(), `SELECT count(*)::text FROM _neutron_migrations`).Scan(&applied); err != nil {
			t.Fatalf("count history: %v", err)
		}
		if applied != "1" {
			t.Fatalf("history must record the migration, got %q", applied)
		}
		var idx string
		if err := fixture.QueryRow(context.Background(), `SELECT count(*)::text FROM pg_indexes WHERE indexname='x01_e2e_docs_emb_hnsw'`).Scan(&idx); err != nil {
			t.Fatalf("count indexes: %v", err)
		}
		if idx != "1" {
			t.Fatalf("hnsw index must exist, got %q", idx)
		}
		// Re-apply is a no-op (up to date).
		code, out = run("migrate", "apply", "--dir", mig)
		if code != 0 || !strings.Contains(out, "up to date") {
			t.Fatalf("re-apply must be a no-op (%d):\n%s", code, out)
		}
	})
}
