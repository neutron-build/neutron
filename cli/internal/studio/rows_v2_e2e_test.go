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

// TestStudioRowProtocolV2E2E exercises the S01 versioned full-key identity
// protocol through REAL handler invocations (direct HTTP, no UI cooperation)
// against a REAL disposable Postgres database, with all state verified by
// independently written SQL oracles.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server; NEUTRON_LIVE_REQUIRED=1 turns a missing URL into a failure. The
// test creates one uniquely-named s01_* database and drops it afterwards.
func TestStudioRowProtocolV2E2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio v2 protocol E2E skipped (set it to a disposable Postgres URL to run)")
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
	seed := []string{
		// Composite PK: two rows share the first component — only the full
		// tuple addresses one row.
		`CREATE TABLE docs (tenant_id int NOT NULL, id int NOT NULL, payload text NOT NULL, PRIMARY KEY (tenant_id, id))`,
		`INSERT INTO docs VALUES (1,1,'alpha'),(1,2,'beta'),(2,1,'gamma')`,
		// Composite FK: full-tuple target.
		`CREATE TABLE orders (tenant_id int NOT NULL, order_no int NOT NULL, label text, PRIMARY KEY (tenant_id, order_no), FOREIGN KEY (tenant_id, order_no) REFERENCES docs (tenant_id, id))`,
		`INSERT INTO orders VALUES (1,1,'o1'),(1,2,'o2')`,
		// Single-column FK for the legacy shape.
		`CREATE TABLE memo (id int PRIMARY KEY, body text, flag text DEFAULT 'DFLT', ts timestamp, tstz timestamptz)`,
		`INSERT INTO memo (id, body) VALUES (1,'original'),(2,'untouched')`,
		`CREATE TABLE memo_refs (rid int PRIMARY KEY, memo_id int REFERENCES memo (id))`,
		// No-key table: honestly read-only.
		`CREATE TABLE keyless (tag text NOT NULL, note text NOT NULL)`,
		`INSERT INTO keyless VALUES ('a','keepme')`,
		// Identity key (generated).
		`CREATE TABLE ident (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, note text NOT NULL)`,
		`INSERT INTO ident (note) VALUES ('gen-row')`,
		// Non-key stored generated column (the B04-L2 carry).
		`CREATE TABLE gen_cols (id int PRIMARY KEY, base int NOT NULL, total int GENERATED ALWAYS AS (base * 2) STORED)`,
		`INSERT INTO gen_cols VALUES (1, 21)`,
		// Typed keys: int8 beyond 2^53, high-scale numeric, bytea, date.
		`CREATE TABLE bigid (id bigint PRIMARY KEY, val text NOT NULL)`,
		`INSERT INTO bigid VALUES (9007199254740993,'nine-quad'),(1,'small')`,
		`CREATE TABLE numkey (k numeric(38,4) PRIMARY KEY, val text NOT NULL)`,
		`INSERT INTO numkey VALUES ('12345678901234567890.1234','exact')`,
		`CREATE TABLE binkey (b bytea PRIMARY KEY, val text NOT NULL)`,
		`INSERT INTO binkey VALUES (decode('00ff10','hex'),'binary')`,
		`CREATE TABLE daykey (d date PRIMARY KEY, ts timestamp, tstz timestamptz)`,
		`INSERT INTO daykey VALUES ('2026-01-02','2026-01-01 19:04:05.678123','2026-03-08 07:30:00.123456+05')`,
		// S01 completion fixtures.
		`CREATE TYPE mood AS ENUM ('sad','ok','happy')`,
		`CREATE TABLE uuidkey (id uuid PRIMARY KEY, val text NOT NULL)`,
		`INSERT INTO uuidkey VALUES ('12345678-9abc-def0-0123-456789abcdef','u'),('12345678-9abc-def0-0123-456789abcdee','neighbour')`,
		`CREATE TABLE enumkey (m mood PRIMARY KEY, val text NOT NULL)`,
		`INSERT INTO enumkey VALUES ('ok','e'),('sad','other')`,
		`CREATE TABLE floatkey (f float8 PRIMARY KEY, val text)`,
		`INSERT INTO floatkey VALUES (0.1,'f')`,
		`CREATE TABLE emptytab (id int PRIMARY KEY, note text)`,
		`CREATE TABLE alldefault (id serial PRIMARY KEY, created text NOT NULL DEFAULT 'auto')`,
		`CREATE TABLE edgekey (d date PRIMARY KEY, val text NOT NULL)`,
		`INSERT INTO edgekey VALUES ('infinity','inf'),('0044-03-15 BC','ides'),('2026-01-01','plain')`,
		`CREATE TABLE tskey (ts timestamptz PRIMARY KEY, val text NOT NULL)`,
		`INSERT INTO tskey VALUES ('0001-01-01 00:00:00.5+00 BC','bc'),('-infinity','neg'),('2026-01-01 00:00:00+00','plain')`,
		`CREATE TABLE neighbours (id bigint PRIMARY KEY, val text NOT NULL)`,
		`INSERT INTO neighbours VALUES (9007199254740992,'even'),(9007199254740993,'odd')`,
		`CREATE TABLE hidden (id int PRIMARY KEY, val text NOT NULL)`,
		`INSERT INTO hidden VALUES (1,'shown'),(2,'secret')`,
		`ALTER TABLE hidden ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY see_some ON hidden USING (id <> 2)`,
		`CREATE TABLE notnullcol (id int PRIMARY KEY, must text NOT NULL)`,
		`INSERT INTO notnullcol VALUES (1,'x')`,
		`CREATE TABLE swapme (id int PRIMARY KEY, val text NOT NULL)`,
		`INSERT INTO swapme VALUES (1,'before')`,
		`CREATE TABLE jsontab (id int PRIMARY KEY, doc jsonb)`,
		`INSERT INTO jsontab VALUES (1, '{"a":1}')`,
	}
	for _, stmt := range seed {
		if err := fixture.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	s := &Server{port: 59999, sessionToken: "s01-e2e-token", clients: map[string]*db.Client{"e2e": fixture}}

	// bindingOf reads the relation binding the way the SPA does (from the
	// table metadata of the current connection instance).
	var bindingOf func(table string) string

	// post sends a v2 mutation. Payloads that do not name a binding get the
	// table's current one injected, so each case exercises the check it is
	// about; binding cases pass their own (stale or forged) value.
	post := func(path, handler string, h http.HandlerFunc, payload string) (int, map[string]any) {
		t.Helper()
		_ = path
		_ = handler
		dec := json.NewDecoder(strings.NewReader(payload))
		dec.UseNumber()
		var generic map[string]any
		if err := dec.Decode(&generic); err == nil {
			if _, has := generic["binding"]; !has {
				if table, ok := generic["table"].(string); ok {
					generic["binding"] = bindingOf(table)
					payload = mustJSON(generic)
				}
			}
		}
		req := httptest.NewRequest(http.MethodPost, "/api/table/v2/x", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://localhost:59999")
		req.Header.Set(sessionHeader, s.sessionToken)
		rec := httptest.NewRecorder()
		h(rec, req)
		var body map[string]any
		if rec.Body.Len() > 0 {
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode response %q: %v", rec.Body.String(), err)
			}
		}
		return rec.Code, body
	}

	update := func(payload string) (int, map[string]any) {
		return post("", "", s.handleTableRowUpdateV2, payload)
	}
	deleteRow := func(payload string) (int, map[string]any) {
		return post("", "", s.handleTableRowDeleteV2, payload)
	}
	insert := func(payload string) (int, map[string]any) {
		return post("", "", s.handleTableRowInsertV2, payload)
	}

	get := func(h http.HandlerFunc, query string) (int, map[string]any) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/table?"+query, nil)
		req.Header.Set("Origin", "http://localhost:59999")
		rec := httptest.NewRecorder()
		h(rec, req)
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode response %q: %v", rec.Body.String(), err)
		}
		return rec.Code, body
	}

	readTable := func(table string) map[string]any {
		t.Helper()
		code, body := get(s.handleTable, "connectionId=e2e&schema=public&table="+table)
		if code != http.StatusOK || body["error"] != nil {
			t.Fatalf("read %s: %d %v", table, code, body["error"])
		}
		return body
	}

	metaFor := func(table string) map[string]any {
		t.Helper()
		code, body := get(s.handleTableRowMetaV2, "connectionId=e2e&schema=public&table="+table)
		if code != http.StatusOK {
			t.Fatalf("meta %s: %d %v", table, code, body)
		}
		return body
	}
	bindingOf = func(table string) string {
		t.Helper()
		b, _ := metaFor(table)["binding"].(string)
		return b
	}

	// cell extracts one row/column cell from a readTable result.
	cell := func(body map[string]any, row int, column string) any {
		t.Helper()
		cols := body["columns"].([]any)
		rows := body["rows"].([]any)
		for i, c := range cols {
			if c == column {
				return rows[row].([]any)[i]
			}
		}
		t.Fatalf("column %q not in %v", column, cols)
		return nil
	}
	versionOf := func(body map[string]any, row int) string {
		t.Helper()
		return body["versions"].([]any)[row].(string)
	}
	tagged := func(v any) (string, string) {
		t.Helper()
		m, ok := v.(map[string]any)
		if !ok {
			t.Fatalf("expected tagged cell, got %v (%T)", v, v)
		}
		return m["t"].(string), m["v"].(string)
	}

	// rowIndexOf finds the row whose first columns match the given key
	// values (reads have no ORDER BY, so position is not identity).
	rowIndexOf := func(body map[string]any, keys ...float64) int {
		t.Helper()
		rows := body["rows"].([]any)
		for i, r := range rows {
			row := r.([]any)
			match := true
			for j, k := range keys {
				if row[j].(float64) != k {
					match = false
					break
				}
			}
			if match {
				return i
			}
		}
		t.Fatalf("no row with key %v in %v", keys, rows)
		return -1
	}
	cellWithValue := func(body map[string]any, column string, value any) int {
		t.Helper()
		cols := body["columns"].([]any)
		for i, c := range cols {
			if c == column {
				for r, row := range body["rows"].([]any) {
					if row.([]any)[i] == value {
						return r
					}
				}
			}
		}
		t.Fatalf("no row with %s = %v", column, value)
		return -1
	}
	// rowWithTaggedValue finds the row whose column carries the tagged wire
	// cell {t, v} (S06: reads are deterministically key-ordered, so these
	// tests look rows up instead of assuming insertion order).
	rowWithTaggedValue := func(body map[string]any, column, tag, value string) int {
		t.Helper()
		cols := body["columns"].([]any)
		for i, c := range cols {
			if c != column {
				continue
			}
			for r, row := range body["rows"].([]any) {
				cell, ok := row.([]any)[i].(map[string]any)
				if ok && cell["t"] == tag && cell["v"] == value {
					return r
				}
			}
		}
		t.Fatalf("no row with %s = %s/%s", column, tag, value)
		return -1
	}

	// textOracle runs an independent SQL query returning one text value.
	textOracle := func(sql string, args ...any) string {
		t.Helper()
		var out string
		if err := fixture.QueryRow(context.Background(), sql, args...).Scan(&out); err != nil {
			t.Fatalf("oracle: %v", err)
		}
		return out
	}
	intOracle := func(sql string) int {
		t.Helper()
		var n int
		if err := fixture.QueryRow(context.Background(), sql).Scan(&n); err != nil {
			t.Fatalf("oracle: %v", err)
		}
		return n
	}

	// ---- read path: key columns, versions, tagged cells ----

	t.Run("table read carries key columns, versions and tagged cells", func(t *testing.T) {
		body := readTable("docs")
		if kc := body["keyColumns"].([]any); len(kc) != 2 || kc[0] != "tenant_id" || kc[1] != "id" {
			t.Errorf("keyColumns = %v, want [tenant_id id]", kc)
		}
		if body["versioned"] != true {
			t.Errorf("versioned = %v, want true", body["versioned"])
		}
		if vs, ok := body["versions"].([]any); !ok || len(vs) != 3 {
			t.Errorf("versions = %v, want 3 entries", body["versions"])
		} else if v, ok := vs[0].(string); !ok || v == "" {
			t.Errorf("versions[0] = %v, want an xmin string", vs[0])
		}
		// Rows AND the column list stay table-width (version stripped).
		if rows := body["rows"].([]any); len(rows) != 3 {
			t.Fatalf("rows = %d, want 3", len(rows))
		} else if len(rows[0].([]any)) != 3 {
			t.Errorf("row width = %d, want 3 (version stripped)", len(rows[0].([]any)))
		}
		if cols := body["columns"].([]any); len(cols) != 3 || cols[2] != "payload" {
			t.Errorf("columns = %v, want [tenant_id id payload] (no xmin column)", cols)
		}
		if b, _ := body["binding"].(string); b == "" || b != bindingOf("docs") {
			t.Errorf("read binding = %v, want the relation binding %q", body["binding"], bindingOf("docs"))
		}
		if body["readOnly"] != false {
			t.Errorf("keyed versioned table readOnly = %v, want false", body["readOnly"])
		}

		big := readTable("bigid")
		idx := rowWithTaggedValue(big, "id", "int8", "9007199254740993")
		tag, v := tagged(cell(big, idx, "id"))
		if tag != "int8" || v != "9007199254740993" {
			t.Errorf("int8 key cell = %s/%s, want int8/9007199254740993", tag, v)
		}
		if c := cell(big, idx, "val"); c != "nine-quad" {
			t.Errorf("plain text cell altered: %v", c)
		}

		num := readTable("numkey")
		tag, v = tagged(cell(num, 0, "k"))
		if tag != "numeric" || v != "12345678901234567890.1234" {
			t.Errorf("numeric key cell = %s/%s", tag, v)
		}

		bin := readTable("binkey")
		tag, v = tagged(cell(bin, 0, "b"))
		if tag != "bytea" || v != "00ff10" {
			t.Errorf("bytea key cell = %s/%s, want bytea/00ff10", tag, v)
		}

		day := readTable("daykey")
		tag, v = tagged(cell(day, 0, "d"))
		if tag != "date" || v != "2026-01-02" {
			t.Errorf("date key cell = %s/%s", tag, v)
		}
		tag, v = tagged(cell(day, 0, "ts"))
		if tag != "timestamp" || v != "2026-01-01T19:04:05.678123" {
			t.Errorf("timestamp cell = %s/%s", tag, v)
		}
		tag, v = tagged(cell(day, 0, "tstz"))
		if tag != "timestamptz" || v != "2026-03-08T02:30:00.123456Z" {
			t.Errorf("timestamptz cell = %s/%s, want UTC canonical", tag, v)
		}
	})

	t.Run("query endpoint tags cells too", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(
			`{"sql":"SELECT id FROM bigid WHERE id > 9007199254740992","connectionId":"e2e"}`))
		req.Header.Set("Origin", "http://localhost:59999")
		req.Header.Set(sessionHeader, s.sessionToken)
		rec := httptest.NewRecorder()
		s.handleQuery(rec, req)
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body["error"] != nil {
			t.Fatalf("query error: %v", body["error"])
		}
		rows := body["rows"].([]any)
		if len(rows) != 1 {
			t.Fatalf("rows = %v", rows)
		}
		tag, v := tagged(rows[0].([]any)[0])
		if tag != "int8" || v != "9007199254740993" {
			t.Errorf("query int8 cell = %s/%s", tag, v)
		}
	})

	// ---- authoritative metadata (B04-L2 carry) ----

	t.Run("meta declares generated and identity columns read-only from the catalog", func(t *testing.T) {
		meta := metaFor("gen_cols")
		cols := meta["columns"].([]any)
		var total, id, base map[string]any
		for _, c := range cols {
			m := c.(map[string]any)
			switch m["name"] {
			case "total":
				total = m
			case "id":
				id = m
			case "base":
				base = m
			}
		}
		if total == nil || id == nil || base == nil {
			t.Fatalf("meta columns missing: %v", cols)
		}
		if total["editable"] != false || !strings.Contains(total["readOnlyReason"].(string), "generated") {
			t.Errorf("generated column must be read-only with reason: %v", total)
		}
		if id["editable"] != false || !strings.Contains(id["readOnlyReason"].(string), "key column") {
			t.Errorf("plain key column must be read-only as the row address: %v", id)
		}
		if base["editable"] != true {
			t.Errorf("plain column must be editable: %v", base)
		}

		identMeta := metaFor("ident")
		for _, c := range identMeta["columns"].([]any) {
			m := c.(map[string]any)
			if m["name"] == "id" && (m["identity"] != true || m["editable"] != false) {
				t.Errorf("identity key metadata wrong: %v", m)
			}
		}
	})

	t.Run("no-key table is honestly read-only", func(t *testing.T) {
		meta := metaFor("keyless")
		if meta["readOnly"] != true {
			t.Fatalf("keyless meta readOnly = %v", meta["readOnly"])
		}
		if reason := meta["readOnlyReason"].(string); !strings.Contains(reason, "no primary key") {
			t.Errorf("readOnlyReason = %q", reason)
		}
		for name, res := range map[string]func() (int, map[string]any){
			"update": func() (int, map[string]any) {
				return update(`{"connectionId":"e2e","schema":"public","table":"keyless","key":[{"column":"tag","value":"a"}],"version":"1","column":"note","value":"x"}`)
			},
			"delete": func() (int, map[string]any) {
				return deleteRow(`{"connectionId":"e2e","schema":"public","table":"keyless","key":[{"column":"tag","value":"a"}],"version":"1"}`)
			},
			"insert": func() (int, map[string]any) {
				return insert(`{"connectionId":"e2e","schema":"public","table":"keyless","values":{"tag":"b","note":"n"}}`)
			},
		} {
			code, body := res()
			if code != http.StatusBadRequest || body["error"] == nil {
				t.Errorf("keyless %s = %d %v, want 400 rejection", name, code, body)
			}
		}
		if n := intOracle(`SELECT count(*) FROM keyless`); n != 1 {
			t.Errorf("keyless rows = %d after rejections, want 1", n)
		}
		if note := textOracle(`SELECT note FROM keyless`); note != "keepme" {
			t.Errorf("keyless note changed: %q", note)
		}
	})

	// ---- full-tuple composite edits (V06 final API) ----

	t.Run("composite full-tuple update changes exactly one row", func(t *testing.T) {
		body := readTable("docs")
		var idx int
		for i, r := range body["rows"].([]any) {
			row := r.([]any)
			if row[0].(float64) == 1 && row[1].(float64) == 2 {
				idx = i
			}
		}
		code, res := update(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"docs",
			"key":[{"column":"tenant_id","value":1},{"column":"id","value":2}],
			"version":%q,"column":"payload","value":"renamed"}`, versionOf(body, idx)))
		if code != http.StatusOK || res["rowsAffected"] != float64(1) {
			t.Fatalf("composite update = %d %v", code, res)
		}
		if res["version"] == "" || res["version"] == versionOf(body, idx) {
			t.Errorf("update must return the NEW row version: %v", res["version"])
		}
		got := textOracle(`SELECT string_agg(payload, ',' ORDER BY tenant_id, id) FROM docs`)
		if got != "alpha,renamed,gamma" {
			t.Errorf("docs = %q, want alpha,renamed,gamma", got)
		}
	})

	t.Run("composite full-tuple delete removes exactly one row", func(t *testing.T) {
		body := readTable("docs")
		idx := rowIndexOf(body, 2, 1)
		code, res := deleteRow(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"docs",
			"key":[{"column":"tenant_id","value":2},{"column":"id","value":1}],
			"version":%q}`, versionOf(body, idx)))
		if code != http.StatusOK || res["rowsAffected"] != float64(1) {
			t.Fatalf("composite delete = %d %v", code, res)
		}
		if n := intOracle(`SELECT count(*) FROM docs`); n != 2 {
			t.Errorf("docs rows = %d, want 2", n)
		}
	})

	t.Run("key subset is rejected with the full tuple named", func(t *testing.T) {
		code, res := update(`{
			"connectionId":"e2e","schema":"public","table":"docs",
			"key":[{"column":"tenant_id","value":1}],"version":"1","column":"payload","value":"X"}`)
		if code != http.StatusBadRequest {
			t.Fatalf("subset update = %d %v, want 400", code, res)
		}
		if msg := res["error"].(string); !strings.Contains(msg, "full primary key (tenant_id, id)") {
			t.Errorf("subset rejection = %q", msg)
		}
		if n := intOracle(`SELECT count(*) FROM docs`); n != 2 {
			t.Errorf("docs rows = %d after rejected subset update", n)
		}
	})

	t.Run("forged non-key column in key tuple is rejected", func(t *testing.T) {
		code, res := update(`{
			"connectionId":"e2e","schema":"public","table":"docs",
			"key":[{"column":"payload","value":"alpha"}],"version":"1","column":"payload","value":"X"}`)
		if code != http.StatusBadRequest {
			t.Fatalf("forged key = %d %v, want 400", code, res)
		}
	})

	// ---- stale-row detection: conflict, never silent overwrite ----

	t.Run("stale version is an explicit conflict and never overwrites", func(t *testing.T) {
		if n := intOracle(`SELECT count(*) FROM memo`); n < 2 {
			t.Fatalf("fixture changed: memo has %d rows", n)
		}
		body := readTable("memo")
		idx := cellWithValue(body, "id", float64(1))
		oldVersion := versionOf(body, idx)
		// External writer (another session) modifies the row after our read.
		if err := fixture.Exec(context.Background(), `UPDATE memo SET body = 'external-write' WHERE id = 1`); err != nil {
			t.Fatal(err)
		}
		current := textOracle(`SELECT xmin::text FROM memo WHERE id = 1`)

		code, res := update(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"memo",
			"key":[{"column":"id","value":1}],"version":%q,"column":"body","value":"CLOBBER"}`, oldVersion))
		if code != http.StatusConflict || res["state"] != "conflict" {
			t.Fatalf("stale update = %d %v, want 409 conflict", code, res)
		}
		if res["currentVersion"] != current {
			t.Errorf("conflict currentVersion = %v, want %q (row's live version)", res["currentVersion"], current)
		}
		if got := textOracle(`SELECT body FROM memo WHERE id = 1`); got != "external-write" {
			t.Errorf("stale write overwrote external value: %q", got)
		}
	})

	t.Run("self-conflict: reusing the pre-edit version after your own edit", func(t *testing.T) {
		body := readTable("memo")
		idx := cellWithValue(body, "id", float64(1))
		oldVersion := versionOf(body, idx)
		code, res := update(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"memo",
			"key":[{"column":"id","value":1}],"version":%q,"column":"body","value":"mine"}`, oldVersion))
		if code != http.StatusOK {
			t.Fatalf("fresh update = %d %v", code, res)
		}
		newVersion := res["version"].(string)
		if newVersion == oldVersion {
			t.Fatalf("update must bump the version")
		}
		// Replaying with the pre-edit version now conflicts.
		code, res = update(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"memo",
			"key":[{"column":"id","value":1}],"version":%q,"column":"body","value":"double"}`, oldVersion))
		if code != http.StatusConflict || res["state"] != "conflict" || res["currentVersion"] != newVersion {
			t.Fatalf("replay = %d %v, want 409 conflict at current version", code, res)
		}
		if got := textOracle(`SELECT body FROM memo WHERE id = 1`); got != "mine" {
			t.Errorf("replayed write applied: %q", got)
		}
	})

	t.Run("missing key is a distinct explicit state", func(t *testing.T) {
		body := readTable("memo")
		_ = body
		code, res := update(`{
			"connectionId":"e2e","schema":"public","table":"memo",
			"key":[{"column":"id","value":99999}],"version":"1","column":"body","value":"ghost"}`)
		if code != http.StatusConflict || res["state"] != "missing" {
			t.Fatalf("missing update = %d %v, want 409 missing", code, res)
		}
		if msg := res["error"].(string); !strings.Contains(msg, "stale, deleted, or not visible") {
			t.Errorf("missing wording must not distinguish hidden from deleted: %q", msg)
		}
	})

	t.Run("stale delete is a conflict", func(t *testing.T) {
		body := readTable("memo")
		idx := cellWithValue(body, "id", float64(1))
		oldVersion := versionOf(body, idx)
		if err := fixture.Exec(context.Background(), `UPDATE memo SET body = 'moved-on' WHERE id = 1`); err != nil {
			t.Fatal(err)
		}
		code, res := deleteRow(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"memo",
			"key":[{"column":"id","value":1}],"version":%q}`, oldVersion))
		if code != http.StatusConflict || res["state"] != "conflict" {
			t.Fatalf("stale delete = %d %v, want 409 conflict", code, res)
		}
		if n := intOracle(`SELECT count(*) FROM memo WHERE id = 1`); n != 1 {
			t.Errorf("stale delete removed the row")
		}
	})

	// ---- typed keys: exactness end to end ----

	t.Run("int8 key beyond 2^53 addresses the exact row", func(t *testing.T) {
		body := readTable("bigid")
		idx := rowWithTaggedValue(body, "id", "int8", "9007199254740993")
		keyCell := cell(body, idx, "id")
		code, res := update(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"bigid",
			"key":[{"column":"id","value":%s}],"version":%q,"column":"val","value":"edited-exact"}`,
			mustJSON(keyCell), versionOf(body, idx)))
		if code != http.StatusOK || res["rowsAffected"] != float64(1) {
			t.Fatalf("bigid update = %d %v", code, res)
		}
		if got := textOracle(`SELECT val FROM bigid WHERE id::text = '9007199254740993'`); got != "edited-exact" {
			t.Errorf("exact-row oracle = %q", got)
		}
		if n := intOracle(`SELECT count(*) FROM bigid WHERE id::text = '1' AND val = 'small'`); n != 1 {
			t.Errorf("unrelated row changed")
		}
	})

	t.Run("numeric, bytea and date keys round-trip exactly", func(t *testing.T) {
		num := readTable("numkey")
		code, res := update(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"numkey",
			"key":[{"column":"k","value":%s}],"version":%q,"column":"val","value":"num-edited"}`,
			mustJSON(cell(num, 0, "k")), versionOf(num, 0)))
		if code != http.StatusOK || res["rowsAffected"] != float64(1) {
			t.Fatalf("numkey update = %d %v", code, res)
		}
		if got := textOracle(`SELECT val FROM numkey WHERE k::text = '12345678901234567890.1234'`); got != "num-edited" {
			t.Errorf("numeric key oracle = %q", got)
		}

		bin := readTable("binkey")
		code, res = update(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"binkey",
			"key":[{"column":"b","value":%s}],"version":%q,"column":"val","value":"bin-edited"}`,
			mustJSON(cell(bin, 0, "b")), versionOf(bin, 0)))
		if code != http.StatusOK || res["rowsAffected"] != float64(1) {
			t.Fatalf("binkey update = %d %v", code, res)
		}
		if got := textOracle(`SELECT val FROM binkey WHERE b = decode('00ff10','hex')`); got != "bin-edited" {
			t.Errorf("bytea key oracle = %q", got)
		}

		day := readTable("daykey")
		code, res = update(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"daykey",
			"key":[{"column":"d","value":%s}],"version":%q,"column":"ts","value":{"t":"timestamp","v":"2027-12-31T23:59:59.999999"}}`,
			mustJSON(cell(day, 0, "d")), versionOf(day, 0)))
		if code != http.StatusOK || res["rowsAffected"] != float64(1) {
			t.Fatalf("daykey update = %d %v", code, res)
		}
		if got := textOracle(`SELECT ts::text FROM daykey WHERE d = '2026-01-02'`); got != "2027-12-31 23:59:59.999999" {
			t.Errorf("timestamp value oracle = %q", got)
		}
	})

	t.Run("timestamptz value edit keeps microseconds across the wire", func(t *testing.T) {
		day := readTable("daykey")
		code, res := update(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"daykey",
			"key":[{"column":"d","value":%s}],"version":%q,"column":"tstz","value":{"t":"timestamptz","v":"2026-06-01T12:00:00.654321Z"}}`,
			mustJSON(cell(day, 0, "d")), versionOf(day, 0)))
		if code != http.StatusOK {
			t.Fatalf("tstz update = %d %v", code, res)
		}
		if got := textOracle(`SELECT extract(microseconds from tstz)::text FROM daykey WHERE d = '2026-01-02'`); got != "654321" {
			t.Errorf("timestamptz microseconds = %q, want 654321", got)
		}
	})

	// ---- insert: NULL / empty / DEFAULT are distinct controls ----

	t.Run("insert distinguishes NULL, empty string and DEFAULT", func(t *testing.T) {
		// flag has DEFAULT 'DFLT'; body is nullable without default.
		code, res := insert(`{"connectionId":"e2e","schema":"public","table":"memo","values":{"id":10,"body":null,"flag":null}}`)
		if code != http.StatusOK {
			t.Fatalf("insert NULL = %d %v", code, res)
		}
		if got := textOracle(`SELECT coalesce(body,'<NULL>') FROM memo WHERE id = 10`); got != "<NULL>" {
			t.Errorf("explicit null body = %q", got)
		}
		if got := textOracle(`SELECT coalesce(flag,'<NULL>') FROM memo WHERE id = 10`); got != "<NULL>" {
			t.Errorf("explicit null flag = %q (must be NULL, not the default)", got)
		}

		code, _ = insert(`{"connectionId":"e2e","schema":"public","table":"memo","values":{"id":11,"body":"","flag":""}}`)
		if code != http.StatusOK {
			t.Fatalf("insert empty = %d", code)
		}
		if got := textOracle(`SELECT length(body)::text || ' ' || length(flag)::text FROM memo WHERE id = 11`); got != "0 0" {
			t.Errorf("empty strings = %q, want both length 0", got)
		}

		code, _ = insert(`{"connectionId":"e2e","schema":"public","table":"memo","values":{"id":12}}`)
		if code != http.StatusOK {
			t.Fatalf("insert default-only = %d", code)
		}
		if got := textOracle(`SELECT flag FROM memo WHERE id = 12`); got != "DFLT" {
			t.Errorf("omitted flag = %q, want DEFAULT 'DFLT'", got)
		}
		if got := textOracle(`SELECT coalesce(body,'<NULL>') FROM memo WHERE id = 12`); got != "<NULL>" {
			t.Errorf("omitted nullable body = %q, want NULL", got)
		}
	})

	t.Run("insert returns the new row identity", func(t *testing.T) {
		code, res := insert(`{"connectionId":"e2e","schema":"public","table":"bigid","values":{"id":{"t":"int8","v":"9007199254740994"},"val":"inserted"}}`)
		if code != http.StatusOK {
			t.Fatalf("insert = %d %v", code, res)
		}
		key := res["key"].([]any)[0].(map[string]any)
		if key["column"] != "id" {
			t.Errorf("identity column = %v", key["column"])
		}
		tag, v := tagged(key["value"])
		if tag != "int8" || v != "9007199254740994" {
			t.Errorf("identity value = %s/%s, want tagged int8", tag, v)
		}
		if res["version"] == "" {
			t.Error("insert must return the new row version")
		}
		if got := textOracle(`SELECT val FROM bigid WHERE id::text = '9007199254740994'`); got != "inserted" {
			t.Errorf("insert oracle = %q", got)
		}
	})

	t.Run("insert into identity column is rejected; omission uses the sequence", func(t *testing.T) {
		code, res := insert(`{"connectionId":"e2e","schema":"public","table":"ident","values":{"id":5,"note":"x"}}`)
		if code != http.StatusBadRequest {
			t.Fatalf("identity insert = %d %v, want 400", code, res)
		}
		code, res = insert(`{"connectionId":"e2e","schema":"public","table":"ident","values":{"note":"created"}}`)
		if code != http.StatusOK {
			t.Fatalf("identity-omitting insert = %d %v", code, res)
		}
		key := res["key"].([]any)[0].(map[string]any)
		tag, v := tagged(key["value"])
		if key["column"] != "id" || tag != "int8" || v == "" {
			t.Errorf("returned identity = %v %s/%s", key["column"], tag, v)
		}
		if got := textOracle(`SELECT note FROM ident WHERE id::text = $1`, v); got != "created" {
			t.Errorf("identity oracle = %q", got)
		}
	})

	t.Run("generated column write is rejected before SQL (B04-L2 carry)", func(t *testing.T) {
		body := readTable("gen_cols")
		code, res := update(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"gen_cols",
			"key":[{"column":"id","value":1}],"version":%q,"column":"total","value":99}`,
			versionOf(body, 0)))
		if code != http.StatusBadRequest || res["error"] == nil {
			t.Fatalf("generated write = %d %v, want 400 pre-SQL rejection", code, res)
		}
		if msg := res["error"].(string); !strings.Contains(msg, "generated") {
			t.Errorf("generated rejection = %q", msg)
		}
		if got := textOracle(`SELECT total::text FROM gen_cols WHERE id = 1`); got != "42" {
			t.Errorf("generated value changed: %q", got)
		}
		// Base column of the same table still edits fine.
		code, res = update(fmt.Sprintf(`{
			"connectionId":"e2e","schema":"public","table":"gen_cols",
			"key":[{"column":"id","value":1}],"version":%q,"column":"base","value":50}`,
			versionOf(body, 0)))
		if code != http.StatusOK {
			t.Fatalf("base edit = %d %v", code, res)
		}
		if got := textOracle(`SELECT total::text FROM gen_cols WHERE id = 1`); got != "100" {
			t.Errorf("generated column did not follow base edit: %q", got)
		}
	})

	// ---- full-tuple FK metadata ----

	t.Run("composite FK carries the complete tuple", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/table/fks?connectionId=e2e&schema=public&table=orders", nil)
		rec := httptest.NewRecorder()
		s.handleTableFKs(rec, req)
		var body struct {
			Fks []struct {
				Name       string   `json:"name"`
				Columns    []string `json:"columns"`
				RefSchema  string   `json:"refSchema"`
				RefTable   string   `json:"refTable"`
				RefColumns []string `json:"refColumns"`
				Composite  bool     `json:"composite"`
				Column     string   `json:"column"`
				RefColumn  string   `json:"refColumn"`
			} `json:"fks"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if len(body.Fks) != 1 {
			t.Fatalf("fks = %+v, want one composite constraint", body.Fks)
		}
		fk := body.Fks[0]
		if !fk.Composite || len(fk.Columns) != 2 || fk.Columns[0] != "tenant_id" || fk.Columns[1] != "order_no" {
			t.Errorf("composite FK columns = %v", fk.Columns)
		}
		if len(fk.RefColumns) != 2 || fk.RefColumns[0] != "tenant_id" || fk.RefColumns[1] != "id" {
			t.Errorf("composite FK refColumns = %v", fk.RefColumns)
		}
		if fk.RefTable != "docs" {
			t.Errorf("refTable = %q", fk.RefTable)
		}
		if fk.Column != "" || fk.RefColumn != "" {
			t.Errorf("legacy per-column fields must be empty for composite FKs: %+v", fk)
		}
	})

	t.Run("single-column FK keeps the legacy shape", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/table/fks?connectionId=e2e&schema=public&table=memo_refs", nil)
		rec := httptest.NewRecorder()
		s.handleTableFKs(rec, req)
		var body struct {
			Fks []struct {
				Columns    []string `json:"columns"`
				RefTable   string   `json:"refTable"`
				RefColumns []string `json:"refColumns"`
				Composite  bool     `json:"composite"`
				Column     string   `json:"column"`
				RefColumn  string   `json:"refColumn"`
			} `json:"fks"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if len(body.Fks) != 1 {
			t.Fatalf("fks = %+v", body.Fks)
		}
		fk := body.Fks[0]
		if fk.Composite || fk.Column != "memo_id" || fk.RefColumn != "id" || fk.RefTable != "memo" {
			t.Errorf("single FK shape = %+v", fk)
		}
	})

	// ---- direct requests validated as strictly as UI (parity) ----

	t.Run("direct requests face identical validation (parity spot checks)", func(t *testing.T) {
		// The exact payloads the UI would send for invalid operations,
		// sent directly: identical outcomes, no API-side trust.
		cases := []struct {
			name    string
			payload string
			wantErr string
		}{
			{"unknown column", `{"connectionId":"e2e","schema":"public","table":"memo","key":[{"column":"id","value":1}],"version":"1","column":"no_such","value":"v"}`, "does not exist"},
			{"key column edit", `{"connectionId":"e2e","schema":"public","table":"memo","key":[{"column":"id","value":1}],"version":"1","column":"id","value":9}`, "key column"},
			{"null key", `{"connectionId":"e2e","schema":"public","table":"memo","key":[{"column":"id","value":null}],"version":"1","column":"body","value":"v"}`, "NULL can never match"},
			{"bad tag payload", `{"connectionId":"e2e","schema":"public","table":"bigid","key":[{"column":"id","value":{"t":"int8","v":"1.5"}}],"version":"1","column":"val","value":"v"}`, "invalid payload"},
			{"non-numeric version", `{"connectionId":"e2e","schema":"public","table":"memo","key":[{"column":"id","value":1}],"version":"soon","column":"body","value":"v"}`, ""},
		}
		for _, tc := range cases {
			code, res := update(tc.payload)
			if code != http.StatusBadRequest || res["error"] == nil {
				t.Errorf("%s: = %d %v, want 400 rejection", tc.name, code, res)
				continue
			}
			if tc.wantErr != "" && !strings.Contains(res["error"].(string), tc.wantErr) {
				t.Errorf("%s: error = %q, want substring %q", tc.name, res["error"], tc.wantErr)
			}
		}
		code, res := insert(`{"connectionId":"e2e","schema":"public","table":"memo","values":{"nope":"x"}}`)
		if code != http.StatusBadRequest || res["error"] == nil || !strings.Contains(res["error"].(string), "does not exist") {
			t.Errorf("unknown insert column = %d %v, want 400 does-not-exist", code, res)
		}
		if n := intOracle(`SELECT count(*) FROM memo WHERE body = 'v' OR body = 'CLOBBER'`); n != 0 {
			t.Errorf("rejected direct requests still wrote rows")
		}
	})

	// ---- origin/token matrix against the live v2 endpoint ----

	t.Run("v2 mutations enforce origin and session token", func(t *testing.T) {
		payload := `{"connectionId":"e2e","binding":"` + bindingOf("memo") + `","schema":"public","table":"memo","key":[{"column":"id","value":1}],"version":"1","column":"body","value":"x"}`

		shoot := func(origin, token string) int {
			req := httptest.NewRequest(http.MethodPost, "/api/table/v2/update", strings.NewReader(payload))
			if origin != "" {
				req.Header.Set("Origin", origin)
			}
			if token != "" {
				req.Header.Set(sessionHeader, token)
			}
			rec := httptest.NewRecorder()
			s.handleTableRowUpdateV2(rec, req)
			return rec.Code
		}
		if code := shoot("http://localhost:59999", s.sessionToken); code != http.StatusConflict {
			t.Errorf("valid auth reaches row-state checks (409 for stale version), got %d", code)
		}
		if code := shoot("http://localhost:1234", s.sessionToken); code != http.StatusForbidden {
			t.Errorf("wrong origin = %d, want 403", code)
		}
		if code := shoot("http://localhost:59999", ""); code != http.StatusForbidden {
			t.Errorf("missing token = %d, want 403", code)
		}
		if code := shoot("http://localhost:59999", "token-from-an-earlier-launch"); code != http.StatusForbidden {
			t.Errorf("replayed cross-session token = %d, want 403", code)
		}
	})

	// ---- S01 completion: binding, strict wire shapes, key types ----

	idx := func(body map[string]any, column string, match func(any) bool) int {
		t.Helper()
		cols := body["columns"].([]any)
		for i, c := range cols {
			if c != column {
				continue
			}
			for r, row := range body["rows"].([]any) {
				if match(row.([]any)[i]) {
					return r
				}
			}
		}
		t.Fatalf("no row in %s matching predicate: %v", column, body["rows"])
		return -1
	}
	taggedValueIs := func(want string) func(any) bool {
		return func(v any) bool {
			m, ok := v.(map[string]any)
			return ok && m["v"] == want
		}
	}

	t.Run("reconnect invalidates identities read before it", func(t *testing.T) {
		body := readTable("memo")
		row := cellWithValue(body, "id", float64(2))
		stale := body["binding"].(string)
		s.mu.Lock()
		s.epochs = map[string]string{"e2e": "reconnected-epoch"}
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			s.epochs = nil
			s.mu.Unlock()
		}()
		code, res := update(fmt.Sprintf(`{"connectionId":"e2e","binding":%q,"schema":"public","table":"memo",
			"key":[{"column":"id","value":2}],"version":%q,"column":"body","value":"after-reconnect"}`, stale, versionOf(body, row)))
		if code != http.StatusConflict || res["state"] != "binding" {
			t.Fatalf("stale-epoch update = %d %v, want 409 binding", code, res)
		}
		if got := textOracle(`SELECT body FROM memo WHERE id = 2`); got != "untouched" {
			t.Errorf("stale-epoch identity wrote: %q", got)
		}
	})

	t.Run("replaced table invalidates identities (relation oid binding)", func(t *testing.T) {
		body := readTable("swapme")
		stale := body["binding"].(string)
		version := versionOf(body, 0)
		for _, stmt := range []string{
			`DROP TABLE swapme`,
			`CREATE TABLE swapme (id int PRIMARY KEY, val text NOT NULL)`,
			`INSERT INTO swapme VALUES (1,'replaced')`,
		} {
			if err := fixture.Exec(context.Background(), stmt); err != nil {
				t.Fatal(err)
			}
		}
		code, res := update(fmt.Sprintf(`{"connectionId":"e2e","binding":%q,"schema":"public","table":"swapme",
			"key":[{"column":"id","value":1}],"version":%q,"column":"val","value":"clobber"}`, stale, version))
		if code != http.StatusConflict || res["state"] != "binding" {
			t.Fatalf("replaced-table update = %d %v, want 409 binding", code, res)
		}
		code, res = deleteRow(fmt.Sprintf(`{"connectionId":"e2e","binding":%q,"schema":"public","table":"swapme",
			"key":[{"column":"id","value":1}],"version":%q}`, stale, version))
		if code != http.StatusConflict || res["state"] != "binding" {
			t.Fatalf("replaced-table delete = %d %v, want 409 binding", code, res)
		}
		if got := textOracle(`SELECT val FROM swapme WHERE id = 1`); got != "replaced" {
			t.Errorf("replaced table row = %q", got)
		}
	})

	t.Run("forged or cross-table binding cannot redirect an operation", func(t *testing.T) {
		body := readTable("memo")
		row := cellWithValue(body, "id", float64(2))
		for name, binding := range map[string]string{
			"forged":        "0:1",
			"other table":   bindingOf("docs"),
			"empty epoch":   ":" + strings.SplitN(bindingOf("memo"), ":", 2)[1],
			"trailing junk": bindingOf("memo") + "0",
		} {
			code, res := update(fmt.Sprintf(`{"connectionId":"e2e","binding":%q,"schema":"public","table":"memo",
				"key":[{"column":"id","value":2}],"version":%q,"column":"body","value":"forged"}`, binding, versionOf(body, row)))
			if code != http.StatusConflict || res["state"] != "binding" {
				t.Errorf("%s binding = %d %v, want 409 binding", name, code, res)
			}
		}
		if got := textOracle(`SELECT body FROM memo WHERE id = 2`); got != "untouched" {
			t.Errorf("forged binding wrote: %q", got)
		}
	})

	t.Run("insert requires the binding too", func(t *testing.T) {
		code, res := insert(`{"connectionId":"e2e","binding":"0:1","schema":"public","table":"memo","values":{"id":50}}`)
		if code != http.StatusConflict || res["state"] != "binding" {
			t.Fatalf("forged-binding insert = %d %v", code, res)
		}
		if n := intOracle(`SELECT count(*) FROM memo WHERE id = 50`); n != 0 {
			t.Errorf("forged-binding insert wrote a row")
		}
	})

	t.Run("int8 neighbours: a rounded key never lands on the adjacent row", func(t *testing.T) {
		body := readTable("neighbours")
		odd := idx(body, "id", taggedValueIs("9007199254740993"))
		// A client that let JSON.parse round the key sends a bare number:
		// refused outright, before SQL.
		code, res := update(fmt.Sprintf(`{"connectionId":"e2e","schema":"public","table":"neighbours",
			"key":[{"column":"id","value":9007199254740993}],"version":%q,"column":"val","value":"rounded"}`, versionOf(body, odd)))
		if code != http.StatusBadRequest {
			t.Fatalf("untagged int8 key = %d %v, want 400", code, res)
		}
		// The exact tagged key changes exactly the odd row.
		code, res = update(fmt.Sprintf(`{"connectionId":"e2e","schema":"public","table":"neighbours",
			"key":[{"column":"id","value":{"t":"int8","v":"9007199254740993"}}],"version":%q,"column":"val","value":"odd-edited"}`, versionOf(body, odd)))
		if code != http.StatusOK {
			t.Fatalf("tagged int8 key = %d %v", code, res)
		}
		if got := textOracle(`SELECT string_agg(val, ',' ORDER BY id) FROM neighbours`); got != "even,odd-edited" {
			t.Errorf("neighbours = %q, want even,odd-edited", got)
		}
		// The interim B04 endpoint decodes numbers exactly too (it used to
		// round 9007199254740993 onto 9007199254740992).
		req := httptest.NewRequest(http.MethodPost, "/api/table/update", strings.NewReader(
			`{"connectionId":"e2e","schema":"public","table":"neighbours","pkColumn":"id","pkValue":9007199254740993,"column":"val","value":"interim"}`))
		req.Header.Set("Origin", "http://localhost:59999")
		req.Header.Set(sessionHeader, s.sessionToken)
		rec := httptest.NewRecorder()
		s.handleTableRowUpdate(rec, req)
		if got := textOracle(`SELECT string_agg(val, ',' ORDER BY id) FROM neighbours`); got != "even,interim" {
			t.Errorf("interim precision: neighbours = %q (response %s), want even,interim", got, rec.Body.String())
		}
	})

	t.Run("uuid and enum keys address exactly one row", func(t *testing.T) {
		body := readTable("uuidkey")
		row := idx(body, "id", func(v any) bool { return v == "12345678-9abc-def0-0123-456789abcdef" })
		code, res := update(fmt.Sprintf(`{"connectionId":"e2e","schema":"public","table":"uuidkey",
			"key":[{"column":"id","value":"12345678-9abc-def0-0123-456789abcdef"}],"version":%q,"column":"val","value":"u2"}`, versionOf(body, row)))
		if code != http.StatusOK {
			t.Fatalf("uuid key = %d %v (uuid must read as a canonical string: %v)", code, res, body["rows"])
		}
		if got := textOracle(`SELECT string_agg(val, ',' ORDER BY id) FROM uuidkey`); got != "neighbour,u2" {
			t.Errorf("uuidkey = %q", got)
		}

		body = readTable("enumkey")
		row = idx(body, "m", func(v any) bool { return v == "ok" })
		code, res = update(fmt.Sprintf(`{"connectionId":"e2e","schema":"public","table":"enumkey",
			"key":[{"column":"m","value":"ok"}],"version":%q,"column":"val","value":"e2"}`, versionOf(body, row)))
		if code != http.StatusOK {
			t.Fatalf("enum key = %d %v", code, res)
		}
		if got := textOracle(`SELECT string_agg(val, ',' ORDER BY m) FROM enumkey`); got != "other,e2" {
			t.Errorf("enumkey = %q", got)
		}
	})

	t.Run("float key is read-only (no exact comparison strategy yet)", func(t *testing.T) {
		meta := metaFor("floatkey")
		if meta["readOnly"] != true || !strings.Contains(meta["readOnlyReason"].(string), "float8") {
			t.Fatalf("floatkey meta = %v", meta)
		}
		if body := readTable("floatkey"); body["readOnly"] != true {
			t.Errorf("floatkey read readOnly = %v", body["readOnly"])
		}
		code, _ := update(`{"connectionId":"e2e","schema":"public","table":"floatkey","key":[{"column":"f","value":0.1}],"version":"1","column":"val","value":"x"}`)
		if code != http.StatusBadRequest {
			t.Errorf("floatkey update = %d, want 400", code)
		}
		if got := textOracle(`SELECT val FROM floatkey`); got != "f" {
			t.Errorf("floatkey changed: %q", got)
		}
	})

	t.Run("infinity and BC temporal keys round-trip exactly", func(t *testing.T) {
		body := readTable("edgekey")
		for _, want := range []string{"infinity", "0044-03-15 BC", "2026-01-01"} {
			row := idx(body, "d", taggedValueIs(want))
			code, res := update(fmt.Sprintf(`{"connectionId":"e2e","schema":"public","table":"edgekey",
				"key":[{"column":"d","value":%s}],"version":%q,"column":"val","value":%q}`,
				mustJSON(cell(body, row, "d")), versionOf(body, row), "hit "+want))
			if code != http.StatusOK {
				t.Errorf("date key %q = %d %v", want, code, res)
			}
		}
		if got := textOracle(`SELECT string_agg(val, ',' ORDER BY d) FROM edgekey`); got != "hit 0044-03-15 BC,hit 2026-01-01,hit infinity" {
			t.Errorf("edgekey = %q", got)
		}

		body = readTable("tskey")
		for _, want := range []string{"-infinity", "0001-01-01T00:00:00.5Z BC"} {
			row := idx(body, "ts", taggedValueIs(want))
			code, res := update(fmt.Sprintf(`{"connectionId":"e2e","schema":"public","table":"tskey",
				"key":[{"column":"ts","value":%s}],"version":%q,"column":"val","value":%q}`,
				mustJSON(cell(body, row, "ts")), versionOf(body, row), "hit"))
			if code != http.StatusOK {
				t.Errorf("timestamptz key %q = %d %v", want, code, res)
			}
		}
		if n := intOracle(`SELECT count(*) FROM tskey WHERE val = 'hit'`); n != 2 {
			t.Errorf("tskey hits = %d, want 2 (plain row untouched)", n)
		}
	})

	t.Run("empty table is editable and all-DEFAULT insert works", func(t *testing.T) {
		if meta := metaFor("emptytab"); meta["readOnly"] != false || meta["versioned"] != true {
			t.Fatalf("empty table meta = %v (the version probe must not need a row)", meta)
		}
		code, res := insert(`{"connectionId":"e2e","schema":"public","table":"emptytab","values":{"id":1,"note":""}}`)
		if code != http.StatusOK {
			t.Fatalf("insert into empty table = %d %v", code, res)
		}
		code, res = insert(`{"connectionId":"e2e","schema":"public","table":"alldefault","values":{}}`)
		if code != http.StatusOK {
			t.Fatalf("all-DEFAULT insert = %d %v", code, res)
		}
		if got := textOracle(`SELECT created FROM alldefault`); got != "auto" {
			t.Errorf("all-DEFAULT row = %q", got)
		}
	})

	t.Run("strict wire shapes are enforced on values", func(t *testing.T) {
		day := readTable("daykey")
		cases := []struct {
			name, payload string
		}{
			{"untagged timestamp value", fmt.Sprintf(`{"connectionId":"e2e","schema":"public","table":"daykey","key":[{"column":"d","value":%s}],"version":%q,"column":"ts","value":"2027-01-01"}`, mustJSON(cell(day, 0, "d")), versionOf(day, 0))},
			{"wrong tag", fmt.Sprintf(`{"connectionId":"e2e","schema":"public","table":"daykey","key":[{"column":"d","value":%s}],"version":%q,"column":"ts","value":{"t":"date","v":"2027-01-01"}}`, mustJSON(cell(day, 0, "d")), versionOf(day, 0))},
			{"object into jsonb", `{"connectionId":"e2e","schema":"public","table":"jsontab","key":[{"column":"id","value":1}],"version":"1","column":"doc","value":{"a":2}}`},
			{"NULL into NOT NULL", `{"connectionId":"e2e","schema":"public","table":"notnullcol","key":[{"column":"id","value":1}],"version":"1","column":"must","isNull":true}`},
		}
		for _, tc := range cases {
			if code, res := update(tc.payload); code != http.StatusBadRequest {
				t.Errorf("%s = %d %v, want 400", tc.name, code, res)
			}
		}
		// JSON null text vs SQL NULL on jsonb stay distinct.
		body := readTable("jsontab")
		code, res := update(fmt.Sprintf(`{"connectionId":"e2e","schema":"public","table":"jsontab","key":[{"column":"id","value":1}],"version":%q,"column":"doc","value":"null"}`, versionOf(body, 0)))
		if code != http.StatusOK {
			t.Fatalf("JSON null update = %d %v", code, res)
		}
		if got := textOracle(`SELECT CASE WHEN doc IS NULL THEN 'SQL NULL' ELSE jsonb_typeof(doc) END FROM jsontab`); got != "null" {
			t.Errorf("JSON null stored as %q, want the JSON null value", got)
		}
		if got := textOracle(`SELECT must FROM notnullcol`); got != "x" {
			t.Errorf("NOT NULL column changed: %q", got)
		}
	})

	t.Run("composite FK follow filters by the whole tuple", func(t *testing.T) {
		code, body := get(s.handleTable, "connectionId=e2e&schema=public&table=docs&match="+
			url.QueryEscape(`[{"column":"tenant_id","value":1},{"column":"id","value":2}]`))
		if code != http.StatusOK || body["error"] != nil {
			t.Fatalf("match read = %d %v", code, body)
		}
		rows := body["rows"].([]any)
		if len(rows) != 1 {
			t.Fatalf("full-tuple match returned %d rows, want exactly 1: %v", len(rows), rows)
		}
		if r := rows[0].([]any); r[0].(float64) != 1 || r[1].(float64) != 2 {
			t.Errorf("matched row = %v, want (1,2,...)", r)
		}
		code, body = get(s.handleTable, "connectionId=e2e&schema=public&table=docs&match="+
			url.QueryEscape(`[{"column":"nope","value":1}]`))
		if code != http.StatusBadRequest {
			t.Errorf("match on unknown column = %d %v, want 400", code, body)
		}
	})

	t.Run("no-key table read reports the read-only state", func(t *testing.T) {
		body := readTable("keyless")
		if body["readOnly"] != true || !strings.Contains(body["readOnlyReason"].(string), "no primary key") {
			t.Errorf("keyless read = readOnly %v reason %v", body["readOnly"], body["readOnlyReason"])
		}
		if _, has := body["versions"]; !has {
			t.Errorf("keyless read should still carry versions (reads are versioned; editing is gated by the key)")
		}
	})

	t.Run("RLS-hidden row is indistinguishable from a missing row", func(t *testing.T) {
		var super bool
		if err := fixture.QueryRow(context.Background(), `SELECT rolsuper OR rolcreaterole FROM pg_roles WHERE rolname = current_user`).Scan(&super); err != nil || !super {
			t.Skip("needs CREATEROLE to build a non-owner role for RLS; skipped with reason")
		}
		role := fmt.Sprintf("s01_rls_%d", os.Getpid())
		for _, stmt := range []string{
			fmt.Sprintf(`CREATE ROLE %q NOLOGIN`, role),
			fmt.Sprintf(`GRANT %q TO current_user`, role),
			fmt.Sprintf(`GRANT SELECT, UPDATE, DELETE ON hidden TO %q`, role),
			fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %q`, role),
		} {
			if err := fixture.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("rls role setup %q: %v", stmt, err)
			}
		}
		defer func() {
			_ = fixture.Exec(context.Background(), fmt.Sprintf(`REVOKE ALL ON hidden FROM %q`, role))
			_ = fixture.Exec(context.Background(), fmt.Sprintf(`REVOKE USAGE ON SCHEMA public FROM %q`, role))
			if err := admin.Exec(context.Background(), fmt.Sprintf(`DROP ROLE IF EXISTS %q`, role)); err != nil {
				t.Errorf("drop role %s: %v", role, err)
			}
		}()
		roleURL, err := url.Parse(dbURL)
		if err != nil {
			t.Fatal(err)
		}
		rq := roleURL.Query()
		rq.Set("options", "-c role="+role)
		roleURL.RawQuery = rq.Encode()
		roleClient, err := db.Connect(context.Background(), roleURL.String())
		if err != nil {
			t.Fatalf("connect as %s: %v", role, err)
		}
		defer roleClient.Close()
		s.mu.Lock()
		s.clients["rls"] = roleClient
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			delete(s.clients, "rls")
			s.mu.Unlock()
		}()

		code, meta := get(s.handleTableRowMetaV2, "connectionId=rls&schema=public&table=hidden")
		if code != http.StatusOK {
			t.Fatalf("rls meta = %d %v", code, meta)
		}
		binding := meta["binding"].(string)
		send := func(id int) (int, map[string]any) {
			return update(fmt.Sprintf(`{"connectionId":"rls","binding":%q,"schema":"public","table":"hidden",
				"key":[{"column":"id","value":%d}],"version":"1","column":"val","value":"probe"}`, binding, id))
		}
		hiddenCode, hiddenRes := send(2)
		absentCode, absentRes := send(99)
		if hiddenCode != http.StatusConflict || absentCode != http.StatusConflict ||
			hiddenRes["state"] != "missing" || absentRes["state"] != "missing" ||
			hiddenRes["error"] != absentRes["error"] || hiddenRes["currentVersion"] != nil {
			t.Errorf("hidden = %d %v; absent = %d %v — must be identical 'missing' outcomes", hiddenCode, hiddenRes, absentCode, absentRes)
		}
		if got := textOracle(`SELECT val FROM hidden WHERE id = 2`); got != "secret" {
			t.Errorf("hidden row changed: %q", got)
		}
	})
}

func mustJSON(v any) string {
	bs, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(bs)
}
