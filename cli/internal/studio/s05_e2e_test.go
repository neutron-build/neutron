package studio

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestStudioSchemaNavDiagnosticsE2E is the S05 leg of V10/V15: schema
// navigation, migration-plan previews from shared metadata, the two carried
// S03/S04 entry-condition regressions, and performance diagnosis — every
// journey through the REAL route table and corsMiddleware against a REAL
// disposable Postgres database with an independent SQL oracle.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server; NEUTRON_LIVE_REQUIRED=1 turns a missing URL into a failure. Uses
// one uniquely named s05_* database, dropped afterwards.
func TestStudioSchemaNavDiagnosticsE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio schema nav e2e skipped")
	}

	dbName := fmt.Sprintf("s05_%d_%d", os.Getpid(), time.Now().Unix())
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
		if !strings.HasPrefix(dbName, "s05_") {
			t.Errorf("refusing to drop unexpected database %q", dbName)
			return
		}
		if err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})

	oracleClient, err := db.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect oracle: %v", err)
	}
	defer oracleClient.Close()
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS "analytics"`,
		`CREATE TABLE users (
			id bigserial PRIMARY KEY,
			email varchar(255) NOT NULL UNIQUE,
			role text NOT NULL DEFAULT 'member',
			seen_at timestamptz,
			created_at timestamptz NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE orders (
			id integer PRIMARY KEY,
			user_id bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			total numeric(12,4),
			note text,
			CONSTRAINT orders_total_positive CHECK (total IS NULL OR total >= 0)
		)`,
		`CREATE INDEX orders_note_idx ON orders (note)`,
		`CREATE TABLE order_lines (
			order_id integer NOT NULL REFERENCES orders(id),
			line_no integer NOT NULL,
			sku text,
			PRIMARY KEY (order_id, line_no)
		)`,
		`CREATE TABLE shipments (
			id integer PRIMARY KEY,
			line_order integer NOT NULL,
			line_number integer NOT NULL,
			CONSTRAINT shipments_line_fkey FOREIGN KEY (line_order, line_number) REFERENCES order_lines (order_id, line_no)
		)`,
		`CREATE VIEW active_users AS SELECT id, email FROM users WHERE seen_at IS NOT NULL`,
		`CREATE VIEW "analytics"."MixedCase View" AS SELECT count(*) AS n FROM orders`,
		`INSERT INTO users (email, seen_at) VALUES ('a@x.test', '2026-09-01T10:00:00Z'::timestamptz), ('b@x.test', NULL)`,
		`INSERT INTO orders VALUES (1, 1, 12.3450, 'first'), (2, 1, NULL, NULL)`,
	} {
		if err := oracleClient.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed: %q: %v", stmt, err)
		}
	}
	oracle := func(query string, dest ...any) {
		t.Helper()
		if err := oracleClient.QueryRow(context.Background(), query).Scan(dest...); err != nil {
			t.Fatalf("oracle %q: %v", query, err)
		}
	}

	studioClient, err := db.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect studio client: %v", err)
	}
	defer studioClient.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	store, err := newConnectionStore()
	if err != nil {
		t.Fatalf("connection store: %v", err)
	}
	s := &Server{
		port:         port,
		sessionToken: "s05-nav-token",
		store:        store,
		clients:      map[string]*db.Client{"e2e": studioClient},
		saved:        &savedQueryStore{path: t.TempDir() + "/studio-saved.json"},
	}
	mux, err := s.routes()
	if err != nil {
		t.Fatalf("routes: %v", err)
	}
	ts := httptest.NewUnstartedServer(s.corsMiddleware(mux))
	ts.Listener = ln
	ts.Start()
	defer ts.Close()

	tokenHeader := map[string]string{"X-Studio-Session": "s05-nav-token", "Content-Type": "application/json", "Origin": fmt.Sprintf("http://localhost:%d", port)}
	get := func(path string) (int, map[string]any) {
		t.Helper()
		res, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer res.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(res.Body).Decode(&body)
		return res.StatusCode, body
	}
	postAuth := func(path string, payload any) (int, map[string]any) {
		t.Helper()
		raw, _ := json.Marshal(payload)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(string(raw)))
		for k, v := range tokenHeader {
			req.Header.Set(k, v)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer res.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(res.Body).Decode(&body)
		return res.StatusCode, body
	}

	// ---------------------------------------------------------------
	t.Run("navigation catalog lists tables and views across schemas", func(t *testing.T) {
		status, body := get("/api/schema?connectionId=e2e")
		if status != 200 {
			t.Fatalf("status %d: %v", status, body)
		}
		raw, _ := json.Marshal(body)
		var sc Schema
		if err := json.Unmarshal(raw, &sc); err != nil {
			t.Fatalf("decode schema: %v", err)
		}
		tables := map[string]bool{}
		for _, tb := range sc.SQL {
			tables[tb.Schema+"."+tb.Name] = true
		}
		if !tables["public.users"] || !tables["public.orders"] {
			t.Fatalf("tables missing: %v", tables)
		}
		views := map[string]bool{}
		for _, v := range sc.Views {
			views[v.Schema+"."+v.Name] = true
		}
		if !views["public.active_users"] || !views["analytics.MixedCase View"] {
			t.Fatalf("views missing: %v", views)
		}
	})

	t.Run("object detail is the introspection truth", func(t *testing.T) {
		status, body := get("/api/schema/object?connectionId=e2e&schema=public&table=orders")
		if status != 200 {
			t.Fatalf("status %d: %v", status, body)
		}
		if body["kind"] != "table" || body["source"] != "introspection-v2" {
			t.Fatalf("kind/source = %v/%v", body["kind"], body["source"])
		}
		tbl, _ := body["table"].(map[string]any)
		constraintTypes := map[string]string{}
		for _, c := range tbl["constraints"].([]any) {
			cons := c.(map[string]any)
			constraintTypes[cons["name"].(string)] = cons["type"].(string)
		}
		if constraintTypes["orders_pkey"] != "primary-key" || constraintTypes["orders_total_positive"] != "check" {
			t.Fatalf("constraints = %v", constraintTypes)
		}
		var wantFKDef string
		oracle(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'orders_user_id_fkey'`, &wantFKDef)
		fkOK := false
		for _, c := range tbl["constraints"].([]any) {
			cons := c.(map[string]any)
			if cons["type"] == "foreign-key" {
				ref := cons["references"].(map[string]any)
				if ref["table"] == "public.users" && ref["onDelete"] == "cascade" && strings.Contains(wantFKDef, "ON DELETE CASCADE") {
					fkOK = true
				}
			}
		}
		if !fkOK {
			t.Fatalf("no FK edge to public.users in %v", tbl["constraints"])
		}
		idxOK := false
		for _, ix := range tbl["indexes"].([]any) {
			idx := ix.(map[string]any)
			if idx["name"] == "orders_note_idx" && idx["method"] == "btree" {
				idxOK = true
			}
		}
		if !idxOK {
			t.Fatalf("orders_note_idx missing from %v", tbl["indexes"])
		}
		// incoming edge from users side
		users, ubody := get("/api/schema/object?connectionId=e2e&schema=public&table=users")
		if users != 200 {
			t.Fatalf("users status %d", users)
		}
		utbl := ubody["table"].(map[string]any)
		if len(utbl["referencedBy"].([]any)) == 0 {
			t.Fatal("users shows no incoming FK relationships")
		}

		// View detail carries the real definition.
		vstatus, vbody := get("/api/schema/object?connectionId=e2e&schema=public&table=active_users")
		if vstatus != 200 || vbody["kind"] != "view" {
			t.Fatalf("view status %d kind %v", vstatus, vbody["kind"])
		}
		def := vbody["view"].(map[string]any)["definition"].(string)
		var wantDef string
		oracle(`SELECT pg_get_viewdef('public.active_users'::regclass)`, &wantDef)
		if strings.TrimSpace(def) != strings.TrimSpace(wantDef) {
			t.Fatalf("view definition = %q, pg_get_viewdef = %q", def, wantDef)
		}

		// Composite relationships keep their ordered tuples on both sides,
		// and edges name the other table by schema and name separately.
		_, sbody := get("/api/schema/object?connectionId=e2e&schema=public&table=shipments")
		out := sbody["table"].(map[string]any)["references"].([]any)
		if len(out) != 1 {
			t.Fatalf("shipments references = %v", out)
		}
		edge := out[0].(map[string]any)
		if edge["schema"] != "public" || edge["name"] != "order_lines" ||
			strings.Join(stringifySlice(edge["columns"]), ",") != "line_order,line_number" ||
			strings.Join(stringifySlice(edge["refColumns"]), ",") != "order_id,line_no" {
			t.Fatalf("composite edge = %v", edge)
		}
		_, lbody := get("/api/schema/object?connectionId=e2e&schema=public&table=order_lines")
		in := lbody["table"].(map[string]any)["referencedBy"].([]any)
		if len(in) != 1 || in[0].(map[string]any)["name"] != "shipments" ||
			strings.Join(stringifySlice(in[0].(map[string]any)["refColumns"]), ",") != "order_id,line_no" {
			t.Fatalf("order_lines referencedBy = %v", in)
		}

		// Dropped relation: honest 404 after a concurrent catalog change.
		if err := oracleClient.Exec(context.Background(), `DROP VIEW "analytics"."MixedCase View"`); err != nil {
			t.Fatalf("drop view: %v", err)
		}
		dstatus, dbody := get("/api/schema/object?connectionId=e2e&schema=analytics&table=" + url.QueryEscape("MixedCase View"))
		if dstatus != http.StatusNotFound {
			t.Fatalf("dropped view status %d body %v", dstatus, dbody)
		}
	})

	t.Run("entry condition 1: execution-time conflicts name operations[N]", func(t *testing.T) {
		// Two-op batch: op 0 updates row 1, op 1 targets row 2 whose
		// version moved under us. The error must name operations[1] — an
		// unprefixed error would focus the innocent first row.
		var staleVersion string
		oracle(`SELECT xmin::text FROM orders WHERE id = 2`, &staleVersion)
		ops := []map[string]any{
			{"op": "update", "schema": "public", "table": "orders", "binding": bindingFor("0", ordersOID(t, oracle)),
				"key":     []map[string]any{{"column": "id", "value": json.Number("1")}},
				"version": staleVersion, "column": "note", "value": "op-zero"},
			{"op": "update", "schema": "public", "table": "orders", "binding": bindingFor("0", ordersOID(t, oracle)),
				"key":     []map[string]any{{"column": "id", "value": json.Number("2")}},
				"version": staleVersion, "column": "note", "value": "op-one"},
		}
		// Make op 1 stale: bump row 2 after reading its version (op 0's
		// row 1 keeps the same version, so op 0 would be innocent but
		// valid).
		if err := oracleClient.Exec(context.Background(), `UPDATE orders SET note = 'moved' WHERE id = 2`); err != nil {
			t.Fatalf("bump: %v", err)
		}
		var row1Ver string
		oracle(`SELECT xmin::text FROM orders WHERE id = 1`, &row1Ver)
		ops[0]["version"] = row1Ver
		status, body := postAuth("/api/table/v2/commit", map[string]any{
			"connectionId": "e2e", "operationId": "s05-ec1-stale-batch", "operations": ops,
		})
		if status != http.StatusConflict {
			t.Fatalf("status %d body %v", status, body)
		}
		msg := body["error"].(string)
		if !strings.Contains(msg, "operations[1]") {
			t.Fatalf("error lacks operations[1] prefix: %q", msg)
		}
		if body["state"] != "conflict" {
			t.Fatalf("state = %v", body["state"])
		}
		// Nothing applied: op 0's row is untouched (atomic rollback).
		var note string
		oracle(`SELECT note FROM orders WHERE id = 1`, &note)
		if note != "first" {
			t.Fatalf("batch was not atomic: row 1 note = %q", note)
		}
	})

	t.Run("entry condition 2: offset-less timestamptz refused on both paths", func(t *testing.T) {
		// Editor-cell path: a commit update carrying an offset-less
		// timestamptz cell on a typed column is a 400 naming the hazard.
		var uver string
		oracle(`SELECT xmin::text FROM users WHERE id = 2`, &uver)
		uop := func(v any) map[string]any {
			return map[string]any{
				"op": "update", "schema": "public", "table": "users", "binding": bindingFor("0", usersOID(t, oracle)),
				"key":     []map[string]any{{"column": "id", "value": map[string]any{"t": "int8", "v": "2"}}},
				"version": uver, "column": "seen_at", "value": v,
			}
		}
		status, body := postAuth("/api/table/v2/commit", map[string]any{
			"connectionId": "e2e", "operationId": "s05-ec2-b",
			"operations": []map[string]any{uop(map[string]any{"t": "timestamptz", "v": "2026-09-24 12:34:56"})},
		})
		if status != http.StatusBadRequest {
			t.Fatalf("editor path status %d body %v", status, body)
		}
		if msg := body["error"].(string); !strings.Contains(msg, "no UTC offset") {
			t.Fatalf("editor path message: %q", msg)
		}
		var stillNull *string
		if err := oracleClient.QueryRow(context.Background(), `SELECT seen_at::text FROM users WHERE id = 2`).Scan(&stillNull); err == nil && stillNull != nil {
			t.Fatalf("refused cell wrote something: %q", *stillNull)
		}

		// Bound-parameter path through /api/query.
		status, body = postAuth("/api/query", map[string]any{
			"connectionId": "e2e", "requestId": "s05-ec2-param",
			"sql":    "UPDATE users SET seen_at = $1 WHERE id = 2",
			"params": []any{map[string]any{"t": "timestamptz", "v": "2026-09-24 12:34:56"}},
		})
		if status != http.StatusBadRequest {
			t.Fatalf("param path status %d body %v", status, body)
		}
		if msg := body["error"].(string); !strings.Contains(msg, "no UTC offset") {
			t.Fatalf("param path message: %q", msg)
		}

		// Explicit offset stores the exact instant (Vancouver session TZ
		// would have shifted it by 8 hours).
		status, _ = postAuth("/api/query", map[string]any{
			"connectionId": "e2e", "requestId": "s05-ec2-ok",
			"sql":    "UPDATE users SET seen_at = $1 WHERE id = 2",
			"params": []any{map[string]any{"t": "timestamptz", "v": "2026-09-24T12:34:56.000001+02:00"}},
		})
		if status != 200 {
			t.Fatal("offset-bearing param update failed")
		}
		var stored string
		oracle(`SELECT to_char(seen_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') FROM users WHERE id = 2`, &stored)
		if stored != "2026-09-24T10:34:56.000001Z" {
			t.Fatalf("stored instant = %q, want 2026-09-24T10:34:56.000001Z (+02:00 offset honored exactly)", stored)
		}
	})

	// plan previews changes and returns the parsed body; it fails the test
	// on a non-200 answer.
	plan := func(t *testing.T, changes []map[string]any) map[string]any {
		t.Helper()
		status, body := postAuth("/api/schema/plan", map[string]any{"connectionId": "e2e", "changes": changes})
		if status != http.StatusOK {
			t.Fatalf("plan status %d body %v", status, body)
		}
		if id, _ := body["planId"].(string); len(id) != 64 {
			t.Fatalf("plan has no planId: %v", body)
		}
		return body
	}
	apply := func(changes []map[string]any, planID string, ack bool) (int, map[string]any) {
		payload := map[string]any{"connectionId": "e2e", "changes": changes, "planId": planID}
		if ack {
			payload["allowDestructive"] = true
		}
		return postAuth("/api/schema/apply", payload)
	}
	columnExists := func(table, column string) bool {
		var n int
		oracle(fmt.Sprintf(`SELECT count(*)::int FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '%s' AND column_name = '%s'`, table, column), &n)
		return n == 1
	}

	t.Run("plan preview uses the CLI planner, rename is a rename, apply is bound to the review", func(t *testing.T) {
		changes := []map[string]any{
			{"op": "rename-column", "schema": "public", "table": "orders", "from": "note", "to": "remark"},
			{"op": "add-column", "schema": "public", "table": "orders", "column": "channel", "type": "varchar(40)", "notNull": false, "default": "'web'"},
		}
		body := plan(t, changes)
		up := stringifySlice(body["up"])
		joined := strings.ToLower(strings.Join(up, "\n"))
		if !strings.Contains(joined, "rename column") || strings.Contains(joined, "drop column") {
			t.Fatalf("rename not planned as a rename (lossy drop+add?): %v", up)
		}
		if body["applied"] != false {
			t.Fatalf("preview claims applied: %v", body)
		}
		if columnExists("orders", "remark") {
			t.Fatal("preview executed DDL")
		}
		// The M03 risk report rides along. The CLI planner drops and
		// recreates views around table alterations; those are the only
		// destructive operations here, no operation loses data, and the
		// planner's warning about the recreate is surfaced verbatim.
		risk := body["risk"].(map[string]any)
		if risk["hasDataLoss"] != false {
			t.Fatalf("risk = %v up=%q", risk, body["up"])
		}
		ops := body["operations"].([]any)
		if len(ops) != len(up) {
			t.Fatalf("operations %d != up %d", len(ops), len(up))
		}
		for _, o := range ops {
			op := o.(map[string]any)
			if op["destructive"] == true && !strings.HasPrefix(op["sql"].(string), "drop view") {
				t.Fatalf("unexpected destructive operation %v", op)
			}
		}
		if !strings.Contains(strings.Join(stringifySlice(body["warnings"]), "\n"), "dropped and recreated around the table alterations") {
			t.Fatalf("view recreate warning not surfaced: %v", body["warnings"])
		}
		// Object identity: the base hash is the live document's hash (the one
		// `neutron schema pull` writes); the target document is valid and
		// hashes to targetSha256.
		live, err := studioClient.IntrospectV2(context.Background())
		if err != nil {
			t.Fatalf("introspect: %v", err)
		}
		if body["baseSha256"] != live.SHA256Hex {
			t.Fatalf("baseSha256 %v != live document %s", body["baseSha256"], live.SHA256Hex)
		}
		rawTarget, _ := json.Marshal(body["target"])
		target, err := db.ParseV2Document(rawTarget)
		if err != nil {
			t.Fatalf("target document invalid: %v", err)
		}
		if target.SHA256Hex != body["targetSha256"] {
			t.Fatalf("target hash %s != targetSha256 %v", target.SHA256Hex, body["targetSha256"])
		}
		if cli := body["cliEquivalent"].(string); !strings.Contains(cli, `--rename 'public.orders.note>public.orders.remark'`) || strings.Contains(cli, "--allow-destructive") {
			t.Fatalf("cliEquivalent = %q", cli)
		}

		// Apply needs the reviewed plan id; a forged id is a stale plan and
		// carries the fresh plan back for review.
		if status, body := apply(changes, "", false); status != http.StatusBadRequest {
			t.Fatalf("apply without planId: %d %v", status, body)
		}
		status, forged := apply(changes, strings.Repeat("0", 64), false)
		if status != http.StatusConflict || forged["state"] != "stale-plan" || forged["plan"] == nil {
			t.Fatalf("forged planId: %d %v", status, forged)
		}
		if columnExists("orders", "remark") {
			t.Fatal("stale plan executed")
		}

		status, applied := apply(changes, body["planId"].(string), true)
		if status != http.StatusOK || applied["applied"] != true || applied["verification"] != "in-sync" {
			t.Fatalf("apply status %d body %v", status, applied)
		}
		var remark, channel string
		oracle(`SELECT remark FROM orders WHERE id = 1`, &remark)
		oracle(`SELECT channel FROM orders WHERE id = 1`, &channel)
		if remark != "first" || channel != "web" {
			t.Fatalf("remark=%q channel=%q (rename lost data or default missing)", remark, channel)
		}
		// The simple index on the renamed column followed the rename.
		var idxDef string
		oracle(`SELECT pg_get_indexdef('orders_note_idx'::regclass)`, &idxDef)
		if !strings.Contains(idxDef, "(remark)") {
			t.Fatalf("index after rename: %s", idxDef)
		}
	})

	t.Run("destructive plans need explicit acknowledgement; dependents are explicit", func(t *testing.T) {
		changes := []map[string]any{{"op": "drop-column", "schema": "public", "table": "orders", "column": "total"}}
		body := plan(t, changes)
		risk := body["risk"].(map[string]any)
		if risk["hasDestructive"] != true || risk["hasDataLoss"] != true {
			t.Fatalf("risk = %v up=%q", risk, body["up"])
		}
		dropFlagged := false
		for _, o := range body["operations"].([]any) {
			op := o.(map[string]any)
			if strings.Contains(op["sql"].(string), `drop column if exists "total"`) || strings.Contains(op["sql"].(string), `drop column "total"`) {
				dropFlagged = op["dataLoss"] == true && op["destructive"] == true
			}
		}
		if !dropFlagged {
			t.Fatalf("drop column not flagged destructive+dataLoss: %v", body["operations"])
		}
		// The check constraint over the column is dropped explicitly, as
		// PostgreSQL would drop it implicitly.
		notes := strings.Join(stringifySlice(body["designerNotes"]), "\n")
		if !strings.Contains(notes, "orders_total_positive") {
			t.Fatalf("designer notes do not name the dependent check constraint: %q", notes)
		}
		if !strings.Contains(body["cliEquivalent"].(string), "--allow-destructive") {
			t.Fatalf("cliEquivalent lacks --allow-destructive: %v", body["cliEquivalent"])
		}
		status, refused := apply(changes, body["planId"].(string), false)
		if status != http.StatusConflict || refused["state"] != "destructive-unacknowledged" {
			t.Fatalf("unacknowledged destructive apply: %d %v", status, refused)
		}
		if !columnExists("orders", "total") {
			t.Fatal("unacknowledged destructive plan executed")
		}
		status, applied := apply(changes, body["planId"].(string), true)
		if status != http.StatusOK || applied["verification"] != "in-sync" {
			t.Fatalf("acknowledged apply: %d %v", status, applied)
		}
		var checks int
		oracle(`SELECT count(*)::int FROM pg_constraint WHERE conname = 'orders_total_positive'`, &checks)
		if columnExists("orders", "total") || checks != 0 {
			t.Fatalf("total still present (%v) or check constraint left (%d)", columnExists("orders", "total"), checks)
		}
	})

	t.Run("plan and apply sit behind the session token and exact origin", func(t *testing.T) {
		body := `{"connectionId":"e2e","changes":[{"op":"add-column","schema":"public","table":"orders","column":"auth_probe","type":"text"}],"planId":"` + strings.Repeat("0", 64) + `"}`
		for _, path := range []string{"/api/schema/plan", "/api/schema/apply"} {
			for name, headers := range map[string]map[string]string{
				"no token":       {"Content-Type": "application/json", "Origin": tokenHeader["Origin"]},
				"foreign origin": {"Content-Type": "application/json", "Origin": "http://localhost:1", "X-Studio-Session": "s05-nav-token"},
			} {
				req, _ := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(body))
				for k, v := range headers {
					req.Header.Set(k, v)
				}
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("%s %s: %v", path, name, err)
				}
				res.Body.Close()
				if res.StatusCode != http.StatusForbidden {
					t.Errorf("%s with %s: status %d, want 403", path, name, res.StatusCode)
				}
			}
		}
		if columnExists("orders", "auth_probe") {
			t.Fatal("unauthenticated request changed the schema")
		}
	})

	t.Run("changes needing CASCADE or rewriting SQL text are refused before planning", func(t *testing.T) {
		cases := []struct {
			name   string
			change map[string]any
			want   string
		}{
			{"drop column used by a view", map[string]any{"op": "drop-column", "schema": "public", "table": "users", "column": "email"}, "active_users"},
			{"rename column used by a view", map[string]any{"op": "rename-column", "schema": "public", "table": "users", "from": "seen_at", "to": "last_seen_at"}, "active_users"},
			{"drop table referenced by a foreign key", map[string]any{"op": "drop-table", "schema": "public", "table": "users"}, "orders_user_id_fkey"},
			{"drop referenced key column", map[string]any{"op": "drop-column", "schema": "public", "table": "order_lines", "column": "line_no"}, "primary key"},
			{"unrepresentable type", map[string]any{"op": "add-column", "schema": "public", "table": "orders", "column": "x", "type": "serial"}, "not representable"},
			{"internal table", map[string]any{"op": "add-column", "schema": "public", "table": "_neutron_migrations", "column": "x", "type": "text"}, "neutron-internal"},
		}
		for _, tc := range cases {
			status, body := postAuth("/api/schema/plan", map[string]any{"connectionId": "e2e", "changes": []map[string]any{tc.change}})
			if status != http.StatusBadRequest || !strings.Contains(fmt.Sprint(body["error"]), tc.want) {
				t.Errorf("%s: status %d body %v (want 400 naming %q)", tc.name, status, body, tc.want)
			}
		}
	})

	t.Run("a failing statement rolls the whole plan back", func(t *testing.T) {
		if err := oracleClient.Exec(context.Background(), `UPDATE orders SET remark = NULL WHERE id = 2`); err != nil {
			t.Fatalf("seed null: %v", err)
		}
		changes := []map[string]any{
			{"op": "add-column", "schema": "public", "table": "orders", "column": "scratch", "type": "text"},
			{"op": "set-not-null", "schema": "public", "table": "orders", "column": "remark"},
		}
		body := plan(t, changes)
		status, failed := apply(changes, body["planId"].(string), true)
		if status != http.StatusUnprocessableEntity || failed["state"] != "sql-error" {
			t.Fatalf("atomicity probe status %d body %v", status, failed)
		}
		if columnExists("orders", "scratch") {
			t.Fatal("failed plan left residue")
		}
	})

	t.Run("create-table and drop-table round trip", func(t *testing.T) {
		create := []map[string]any{
			{"op": "create-table", "schema": "analytics", "table": "tags", "columns": []map[string]any{
				{"name": "id", "type": "bigint", "notNull": true, "isPrimaryKey": true},
				{"name": "label", "type": "varchar(60)", "notNull": true, "default": "'untagged'"},
			}},
		}
		body := plan(t, create)
		status, applied := apply(create, body["planId"].(string), false)
		if status != http.StatusOK || applied["applied"] != true || applied["verification"] != "in-sync" {
			t.Fatalf("create-table apply status %d body %v", status, applied)
		}
		var def string
		oracle(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'analytics.tags'::regclass AND contype = 'p'`, &def)
		if def != "PRIMARY KEY (id)" {
			t.Fatalf("tags PK = %q", def)
		}
		drop := []map[string]any{{"op": "drop-table", "schema": "analytics", "table": "tags"}}
		body = plan(t, drop)
		status, applied = apply(drop, body["planId"].(string), true)
		if status != http.StatusOK || applied["applied"] != true {
			t.Fatalf("drop-table apply status %d body %v", status, applied)
		}
		var exists bool
		oracle(`SELECT to_regclass('analytics.tags') IS NOT NULL`, &exists)
		if exists {
			t.Fatal("tags still exists after acknowledged drop")
		}
	})

	t.Run("catalog refresh: benign concurrent changes apply, changed plans are stale, vanished targets refuse", func(t *testing.T) {
		// Benign: an unrelated table appears between review and apply. The
		// fresh plan's statements are identical, so the reviewed plan runs.
		changes := []map[string]any{{"op": "add-column", "schema": "public", "table": "orders", "column": "ephemeral", "type": "text"}}
		body := plan(t, changes)
		if err := oracleClient.Exec(context.Background(), `CREATE TABLE concurrent_noise (x int)`); err != nil {
			t.Fatalf("concurrent add: %v", err)
		}
		status, applied := apply(changes, body["planId"].(string), true)
		if status != http.StatusOK || applied["verification"] != "in-sync" || !columnExists("orders", "ephemeral") {
			t.Fatalf("benign concurrent apply status %d body %v", status, applied)
		}

		// Changed: another session makes the reviewed statement moot. The
		// fresh plan differs, so the apply refuses and returns it for review.
		changes = []map[string]any{{"op": "set-not-null", "schema": "public", "table": "orders", "column": "channel"}}
		body = plan(t, changes)
		if err := oracleClient.Exec(context.Background(), `ALTER TABLE orders ALTER COLUMN channel SET NOT NULL`); err != nil {
			t.Fatalf("concurrent alter: %v", err)
		}
		status, stale := apply(changes, body["planId"].(string), true)
		if status != http.StatusConflict || stale["state"] != "stale-plan" {
			t.Fatalf("changed plan: status %d body %v", status, stale)
		}
		fresh := stale["plan"].(map[string]any)
		if len(stringifySlice(fresh["up"])) != 0 {
			t.Fatalf("fresh plan should be empty (already NOT NULL): %v", fresh["up"])
		}

		// Vanished: the target column is renamed away. Planning refuses and
		// names it; nothing executes.
		changes = []map[string]any{{"op": "alter-column-type", "schema": "public", "table": "orders", "column": "remark", "type": "varchar(80)"}}
		body = plan(t, changes)
		if err := oracleClient.Exec(context.Background(), `ALTER TABLE orders RENAME COLUMN remark TO remark_moved`); err != nil {
			t.Fatalf("concurrent rename: %v", err)
		}
		status, gone := apply(changes, body["planId"].(string), true)
		if status != http.StatusBadRequest || !strings.Contains(fmt.Sprint(gone["error"]), `"remark" does not exist`) {
			t.Fatalf("vanished target: status %d body %v", status, gone)
		}
		var remarkType string
		oracle(`SELECT data_type FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'remark_moved'`, &remarkType)
		if remarkType != "text" {
			t.Fatalf("stale plan executed against a changed catalog (type now %q)", remarkType)
		}
		if err := oracleClient.Exec(context.Background(), `ALTER TABLE orders RENAME COLUMN remark_moved TO remark`); err != nil {
			t.Fatalf("restore: %v", err)
		}
	})

	t.Run("apply refuses migration-managed databases and a held migration lock", func(t *testing.T) {
		changes := []map[string]any{{"op": "add-column", "schema": "public", "table": "orders", "column": "managed_probe", "type": "text"}}
		body := plan(t, changes)

		// A running migration holds the runner lock: apply waits (bounded)
		// then refuses; nothing executes.
		holder, err := oracleClient.LockMigrations(context.Background())
		if err != nil {
			t.Fatalf("hold lock: %v", err)
		}
		prevWait := schemaApplyLockWait
		schemaApplyLockWait = 300 * time.Millisecond
		status, locked := apply(changes, body["planId"].(string), false)
		schemaApplyLockWait = prevWait
		holder.Release()
		if status != http.StatusConflict || locked["state"] != "locked" || columnExists("orders", "managed_probe") {
			t.Fatalf("held lock: status %d body %v", status, locked)
		}

		// A database with migration history takes changes through migration
		// files, exactly like `neutron db push` without --force.
		for _, stmt := range []string{
			`CREATE TABLE _neutron_migrations (version text PRIMARY KEY)`,
			`INSERT INTO _neutron_migrations VALUES ('001')`,
		} {
			if err := oracleClient.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("seed history: %v", err)
			}
		}
		defer func() {
			if err := oracleClient.Exec(context.Background(), `DROP TABLE _neutron_migrations`); err != nil {
				t.Errorf("cleanup history: %v", err)
			}
		}()
		status, managed := apply(changes, body["planId"].(string), false)
		if status != http.StatusConflict || managed["state"] != "migration-managed" || columnExists("orders", "managed_probe") {
			t.Fatalf("migration-managed: status %d body %v", status, managed)
		}
		// The preview still works there (it never executes).
		plan(t, changes)
	})

	t.Run("slow-query diagnosis from the duration log", func(t *testing.T) {
		// A genuinely slow statement and a fast one, through the editor.
		status, _ := postAuth("/api/query", map[string]any{
			"connectionId": "e2e", "requestId": "s05-slow-1",
			"sql": "SELECT pg_sleep(0.4), count(*) FROM orders",
		})
		if status != 200 {
			t.Fatal("slow query failed")
		}
		postAuth("/api/query", map[string]any{
			"connectionId": "e2e", "requestId": "s05-fast-1", "sql": "SELECT 1",
		})
		// A failing statement is recorded with state error.
		postAuth("/api/query", map[string]any{
			"connectionId": "e2e", "requestId": "s05-err-1", "sql": "SELECT 1/0",
		})

		status, body := get("/api/diagnostics/queries?connectionId=e2e&minMs=300&limit=50")
		if status != 200 {
			t.Fatalf("diagnostics status %d: %v", status, body)
		}
		entries := body["entries"].([]any)
		var sawSlow bool
		for _, e := range entries {
			entry := e.(map[string]any)
			if entry["requestId"] == "s05-slow-1" {
				sawSlow = true
				if entry["durationMs"].(float64) < 350 {
					t.Fatalf("slow entry duration = %v", entry["durationMs"])
				}
				if !strings.Contains(entry["sql"].(string), "pg_sleep") {
					t.Fatalf("slow entry sql = %v", entry["sql"])
				}
				if entry["state"] != "ok" {
					t.Fatalf("slow entry state = %v", entry["state"])
				}
			}
			if entry["requestId"] == "s05-fast-1" {
				t.Fatal("minMs filter leaked a fast statement")
			}
		}
		if !sawSlow {
			t.Fatalf("slow statement not in diagnostics entries: %v", entries)
		}

		status, body = get("/api/diagnostics/queries?connectionId=e2e&minMs=0&limit=500")
		if status != 200 {
			t.Fatalf("full diagnostics status %d", status)
		}
		states := map[string]bool{}
		for _, e := range body["entries"].([]any) {
			states[e.(map[string]any)["state"].(string)] = true
		}
		if !states["error"] {
			t.Fatalf("failing statement not recorded with state error: %v", states)
		}
		if _, hasScope := body["scope"]; !hasScope {
			t.Fatal("diagnostics response lacks the honest scope statement")
		}
		// pg_stat_statements: available flag present either way (probe
		// result depends on the server; the reason must be honest when
		// unavailable).
		pss, ok := body["pgStatStatements"].(map[string]any)
		if !ok || (pss["available"] != true && pss["reason"] == nil) {
			t.Fatalf("pg_stat_statements probe missing or dishonest: %v", body["pgStatStatements"])
		}
	})

	t.Run("table statistics and index usage match the catalog", func(t *testing.T) {
		// Drive reads through the editor, then bracket the endpoint's
		// answer between two independent catalog reads: the counters are
		// cumulative and flushed asynchronously by each backend, so the
		// endpoint must fall between the oracle's before/after values.
		for _, q := range []string{"SELECT remark FROM orders WHERE id = 1", "SELECT count(*) FROM orders"} {
			if _, body := postAuth("/api/query", map[string]any{"connectionId": "e2e", "sql": q}); body["error"] != nil {
				t.Fatalf("%s: %v", q, body["error"])
			}
		}
		var seqBefore, idxBefore, seqAfter, idxAfter int64
		oracle(`SELECT seq_scan, COALESCE(idx_scan, 0) FROM pg_stat_user_tables WHERE relid = 'public.orders'::regclass`, &seqBefore, &idxBefore)
		status, body := get("/api/diagnostics/table-stats?connectionId=e2e&schema=public&table=orders")
		if status != 200 {
			t.Fatalf("table-stats status %d body %v", status, body)
		}
		oracle(`SELECT seq_scan, COALESCE(idx_scan, 0) FROM pg_stat_user_tables WHERE relid = 'public.orders'::regclass`, &seqAfter, &idxAfter)
		stats := body["stats"].(map[string]any)
		if got := int64(stats["seqScan"].(float64)); got < seqBefore || got > seqAfter {
			t.Fatalf("seqScan = %d outside oracle bracket [%d, %d]", got, seqBefore, seqAfter)
		}
		if got := int64(stats["idxScan"].(float64)); got < idxBefore || got > idxAfter {
			t.Fatalf("idxScan = %d outside oracle bracket [%d, %d]", got, idxBefore, idxAfter)
		}
		var wantSize int64
		oracle(`SELECT pg_total_relation_size('public.orders'::regclass)`, &wantSize)
		if int64(body["totalSizeBytes"].(float64)) != wantSize {
			t.Fatalf("totalSizeBytes = %v want %d", body["totalSizeBytes"], wantSize)
		}

		// A table without indexes reports idx_scan NULL in the catalog; the
		// endpoint must answer 0, not fail, and quoted names must resolve.
		if err := oracleClient.Exec(context.Background(), `CREATE TABLE "Mixed Stats" (v int)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		status, mixed := get("/api/diagnostics/table-stats?connectionId=e2e&schema=public&table=" + url.QueryEscape("Mixed Stats"))
		if status != 200 || mixed["stats"].(map[string]any)["idxScan"].(float64) != 0 || mixed["totalSizeBytes"] == nil {
			t.Fatalf("index-less mixed-case table: status %d body %v", status, mixed)
		}
		var idxScansFound bool
		for _, ix := range body["indexes"].([]any) {
			entry := ix.(map[string]any)
			if entry["name"] == "orders_note_idx" {
				idxScansFound = true
				if !strings.Contains(entry["definition"].(string), "CREATE INDEX") {
					t.Fatalf("index definition = %v", entry["definition"])
				}
			}
		}
		if !idxScansFound {
			t.Fatalf("orders_note_idx missing from stats: %v", body["indexes"])
		}
		if _, ok := body["notes"]; !ok {
			t.Fatal("stats response lacks honest counter notes")
		}

		// Unknown table: honest 404.
		status, body = get("/api/diagnostics/table-stats?connectionId=e2e&schema=public&table=nope")
		if status != http.StatusNotFound {
			t.Fatalf("unknown table stats status %d body %v", status, body)
		}
	})

	t.Run("EXPLAIN ANALYZE plan carries buffers for the tree view", func(t *testing.T) {
		status, body := postAuth("/api/query/explain", map[string]any{
			"connectionId": "e2e", "requestId": "s05-explain-buffers",
			"sql":     "SELECT * FROM orders WHERE remark = 'first'",
			"analyze": true,
		})
		if status != 200 {
			t.Fatalf("explain status %d body %v", status, body)
		}
		raw, _ := json.Marshal(body["plan"])
		if !strings.Contains(string(raw), "Blocks") {
			// BUFFERS appears as "Shared Hit Blocks"/"Shared Read Blocks"/...
			// keys in FORMAT JSON.
			t.Fatalf("ANALYZE plan lacks BUFFERS data: %s", string(raw)[:400])
		}
		if body["executed"] != true || body["committed"] != false {
			t.Fatalf("explain semantics: %v", body)
		}
	})
}

func ordersOID(t *testing.T, oracle func(string, ...any)) uint32 {
	t.Helper()
	var oid uint32
	oracle(`SELECT 'public.orders'::regclass::oid`, &oid)
	return oid
}

func usersOID(t *testing.T, oracle func(string, ...any)) uint32 {
	t.Helper()
	var oid uint32
	oracle(`SELECT 'public.users'::regclass::oid`, &oid)
	return oid
}

func stringifySlice(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, item.(string))
	}
	return out
}
