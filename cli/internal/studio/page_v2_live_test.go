package studio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// Native PostgreSQL only. Owns and drops one generated disposable database;
// never reads/persists the user's saved connection store or launches a browser.
func TestStudioKeysetPageNative(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_E2E_DATABASE_URL required")
		}
		t.Skip("native disposable PostgreSQL URL absent")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := db.Connect(ctx, base)
	if err != nil {
		t.Fatal("admin connect failed")
	}
	t.Cleanup(admin.Close)
	name := fmt.Sprintf("page_v2_%d_%d", os.Getpid(), time.Now().UnixNano())
	if err = admin.Exec(ctx, "CREATE DATABASE "+quoteIdent(name)); err != nil {
		t.Fatal("create disposable database failed")
	}
	t.Cleanup(func() {
		cleanup, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		if !strings.HasPrefix(name, "page_v2_") {
			t.Error("cleanup ownership refused")
			return
		}
		if err := admin.Exec(cleanup, "DROP DATABASE "+quoteIdent(name)+" WITH (FORCE)"); err != nil {
			t.Error("drop owned disposable database failed")
		}
	})
	fixtureURL, err := url.Parse(deriveStudioDatabaseURL(t, base, name))
	if err != nil {
		t.Fatal("fixture URL invalid")
	}
	startup := fixtureURL.Query()
	startup.Set("options", "-c search_path=public,pg_catalog")
	fixtureURL.RawQuery = startup.Encode()
	fixture, err := db.Connect(ctx, fixtureURL.String())
	if err != nil {
		t.Fatal("fixture connect failed")
	}
	t.Cleanup(fixture.Close)
	stmts := []string{
		`CREATE SCHEMA "Odd Schema"`,
		`CREATE DOMAIN public.text AS pg_catalog.text`,
		`CREATE TABLE "Odd Schema"."Odd Table" ("Key" bigint PRIMARY KEY, exact numeric(30,4), body text, raw bytea)`,
		`INSERT INTO "Odd Schema"."Odd Table" VALUES (-9223372036854775808,123456789012345678901234.1234,'minimum',decode('00ff','hex')),(-1,NULL,'negative',NULL),(9007199254740993,42.0100,'above JS exact integer',decode('ff','hex')),(9223372036854775807,0.0001,'maximum',NULL)`,
		`CREATE TABLE public."Odd Table" ("Key" bigint PRIMARY KEY, body text)`,
		`INSERT INTO public."Odd Table" VALUES (1,'decoy')`,
		`CREATE TABLE public.int_key (id int PRIMARY KEY)`,
		`CREATE DOMAIN public.int8 AS bigint`,
		`CREATE TABLE public.domain_key (id public.int8 PRIMARY KEY)`,
		`CREATE TABLE public.composite_key (a bigint,b bigint, PRIMARY KEY(a,b))`,
		`CREATE TABLE public.parent_key (id bigint PRIMARY KEY)`,
		`CREATE TABLE public.child_key () INHERITS(public.parent_key)`,
	}
	for _, stmt := range stmts {
		if err = fixture.Exec(ctx, stmt); err != nil {
			t.Fatal("seed failed")
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listen failed")
	}
	s := &Server{port: ln.Addr().(*net.TCPAddr).Port, sessionToken: "page-test-session", clients: map[string]*db.Client{"native": fixture}, epochs: map[string]string{"native": "page-epoch"}}
	mux, err := s.routes()
	if err != nil {
		t.Fatal("routes failed")
	}
	ts := httptest.NewUnstartedServer(s.corsMiddleware(mux))
	ts.Listener = ln
	ts.Start()
	t.Cleanup(ts.Close)
	httpClient := &http.Client{Timeout: 10 * time.Second}
	call := func(p pageRequest, token string) (int, map[string]any) {
		t.Helper()
		b, _ := json.Marshal(p)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/api/table/v2/page", bytes.NewReader(b))
		if err != nil {
			t.Fatal("request failed")
		}
		req.Header.Set(sessionHeader, token)
		req.Header.Set("Origin", ts.URL)
		res, err := httpClient.Do(req)
		if err != nil {
			t.Fatal("page HTTP failed")
		}
		defer res.Body.Close()
		payload, err := io.ReadAll(io.LimitReader(res.Body, pageRowBytes+1))
		if err != nil || len(payload) > pageRowBytes {
			t.Fatal("invalid response budget")
		}
		var result map[string]any
		if err = json.Unmarshal(payload, &result); err != nil {
			t.Fatal("invalid JSON response")
		}
		return res.StatusCode, result
	}
	p := pageRequest{ConnectionID: "native", Schema: "Odd Schema", Table: "Odd Table", Profile: "postgres-direct", Limit: 2}
	if status, _ := call(p, ""); status != 403 {
		t.Fatalf("missing session status%d", status)
	}
	status, first := call(p, s.sessionToken)
	if status != 200 {
		t.Fatalf("first page status%d: %v", status, first)
	}
	keys := func(result map[string]any) []string {
		t.Helper()
		rows, ok := result["rows"].([]any)
		if !ok {
			t.Fatal("missing rows")
		}
		out := []string{}
		for _, v := range rows {
			row := v.([]any)
			cell := row[0].(map[string]any)
			if cell["t"] != "int8" {
				t.Fatal("lossy key wire")
			}
			out = append(out, cell["v"].(string))
		}
		return out
	}
	if got := strings.Join(keys(first), ","); got != "-9223372036854775808,-1" {
		t.Fatalf("first keys %s", got)
	}
	var physicalOID uint32
	if err = fixture.QueryRow(ctx, `SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='Odd Schema' AND c.relname='Odd Table'`).Scan(&physicalOID); err != nil || physicalOID == 0 {
		t.Fatal("physical relation oracle failed")
	}
	if first["hasNext"] != true || first["binding"] != bindingFor("page-epoch", physicalOID) {
		t.Fatal("binding differs from physical relation oracle")
	}

	if first["consistency"] != "live-keyset/request-repeatable-read" {
		t.Fatal("consistency claim changed")
	}
	if _, ok := first["totalCount"]; ok {
		t.Fatal("unexpected full count")
	}
	row := first["rows"].([]any)[0].([]any)
	if row[1].(map[string]any)["v"] != "123456789012345678901234.1234" || row[3].(map[string]any)["t"] != "bytea" {
		t.Fatal("exact wire codecs lost")
	}
	versions := first["versions"].([]any)
	var xmin string
	if err = fixture.QueryRow(ctx, `SELECT xmin::text FROM "Odd Schema"."Odd Table" WHERE "Key"=-9223372036854775808`).Scan(&xmin); err != nil || versions[0] != xmin {
		t.Fatal("authoritative version mismatch")
	}
	cursor := first["nextCursor"].(string)
	p.Cursor = cursor
	status, second := call(p, s.sessionToken)
	if status != 200 || strings.Join(keys(second), ",") != "9007199254740993,9223372036854775807" || second["hasNext"] != false || second["nextCursor"] != "" {
		t.Fatalf("second page incorrect: status%d %v", status, second)
	}
	// Continuation is live: a newly inserted key behind boundary is not claimed
	// to be part of a shared snapshot; a later value ahead is observed.
	if err = fixture.Exec(ctx, `INSERT INTO "Odd Schema"."Odd Table" VALUES(-2,NULL,'behind',NULL); UPDATE "Odd Schema"."Odd Table" SET body='later committed value' WHERE "Key"=9007199254740993`); err != nil {
		t.Fatal("concurrent fixture writes failed")
	}
	status, later := call(p, s.sessionToken)
	if status != 200 || strings.Join(keys(later), ",") != "9007199254740993,9223372036854775807" || later["rows"].([]any)[0].([]any)[2] != "later committed value" {
		t.Fatal("live consistency behavior incorrect")
	}
	changed := p
	changed.Limit = 3
	if status, _ = call(changed, s.sessionToken); status != 409 {
		t.Fatal("changed plan cursor accepted")
	}
	changed = p
	changed.Schema = "public"
	if status, _ = call(changed, s.sessionToken); status != 409 {
		t.Fatal("qualified decoy cursor accepted")
	}
	changed = p
	changed.Cursor = cursor + "x"
	if status, _ = call(changed, s.sessionToken); status != 409 {
		t.Fatal("tampered cursor accepted")
	}
	s.mu.Lock()
	s.epochs["native"] = "new-epoch"
	s.mu.Unlock()
	if status, _ = call(p, s.sessionToken); status != 409 {
		t.Fatal("reconnect cursor accepted")
	}
	s.mu.Lock()
	s.epochs["native"] = "page-epoch"
	s.mu.Unlock()
	if err = fixture.Exec(ctx, `ALTER TABLE "Odd Schema"."Odd Table" ADD COLUMN new_column text`); err != nil {
		t.Fatal("alter failed")
	}
	if status, _ = call(p, s.sessionToken); status != 409 {
		t.Fatal("same-OID definition drift accepted")
	}
	p.Cursor = ""
	status, fresh := call(p, s.sessionToken)
	if status != 200 {
		t.Fatal("new plan after DDL failed")
	}
	oldCursor := fresh["nextCursor"].(string)
	if err = fixture.Exec(ctx, `DROP TABLE "Odd Schema"."Odd Table";CREATE TABLE "Odd Schema"."Odd Table"("Key" bigint PRIMARY KEY,body text)`); err != nil {
		t.Fatal("recreate failed")
	}
	p.Cursor = oldCursor
	if status, _ = call(p, s.sessionToken); status != 409 {
		t.Fatal("replacement relation cursor accepted")
	}
	for _, table := range []string{"int_key", "domain_key", "composite_key", "parent_key", "child_key"} {
		changed := pageRequest{ConnectionID: "native", Schema: "public", Table: table, Profile: "postgres-direct", Limit: 2}
		if status, _ = call(changed, s.sessionToken); status != 400 {
			t.Fatalf("unsupported %s status%d", table, status)
		}
	}
	if err = fixture.Exec(ctx, `CREATE TABLE public.huge(id bigint PRIMARY KEY, body text);INSERT INTO public.huge VALUES(1,repeat('x',8388609))`); err != nil {
		t.Fatal("huge fixture failed")
	}
	huge := pageRequest{ConnectionID: "native", Schema: "public", Table: "huge", Profile: "postgres-direct", Limit: 1}
	if status, _ = call(huge, s.sessionToken); status != 413 {
		t.Fatalf("huge row status%d", status)
	}
}
