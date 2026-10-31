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
	"net/http/httptrace"
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
	readerRole := name + "_reader"
	roleCreated := false
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
		if roleCreated {
			if err := admin.Exec(cleanup, "DROP ROLE "+quoteIdent(readerRole)); err != nil {
				t.Error("drop owned reader role failed")
			}
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
		`CREATE TABLE "Odd Schema"."Odd Table" ("Key" bigint PRIMARY KEY, exact numeric(30,4), body pg_catalog.text, raw bytea)`,
		`INSERT INTO "Odd Schema"."Odd Table" VALUES (-9223372036854775808,123456789012345678901234.1234,'minimum',decode('00ff','hex')),(-1,NULL,'negative',NULL),(9007199254740993,42.0100,'above JS exact integer',decode('ff','hex')),(9223372036854775807,0.0001,'maximum',NULL)`,
		`CREATE TABLE public."Odd Table" ("Key" bigint PRIMARY KEY, body pg_catalog.text)`,
		`INSERT INTO public."Odd Table" VALUES (1,'decoy')`,
		`CREATE TABLE public.int_key (id int PRIMARY KEY)`,
		`CREATE TABLE public.json_precision (id bigint PRIMARY KEY, payload jsonb)`,
		`INSERT INTO public.json_precision VALUES(1,'{"large":9007199254740993}'::jsonb),(2,'null'::jsonb),(3,NULL)`,
		`CREATE TABLE public.json_text (id bigint PRIMARY KEY, payload json)`,
		`INSERT INTO public.json_text VALUES(1,'{"large":9007199254740993}'::json),(2,'null'::json),(3,NULL)`,
		`CREATE TABLE public.array_value(id bigint PRIMARY KEY, payload bigint[])`,
		`CREATE TABLE public.custom_value(id bigint PRIMARY KEY, payload public.text)`,

		`CREATE DOMAIN public.int8 AS bigint`,
		`CREATE TABLE public.domain_key (id public.int8 PRIMARY KEY)`,
		`CREATE TABLE public.composite_key (a bigint,b bigint, PRIMARY KEY(a,b))`,
		`CREATE TABLE public.parent_key (id bigint PRIMARY KEY)`,
		`CREATE TABLE public.child_key () INHERITS(public.parent_key)`,
		`CREATE TABLE public.large_sparse (id bigint PRIMARY KEY, body pg_catalog.text)`,
		`INSERT INTO public.large_sparse SELECT 9007199254740993::bigint+g*100003::bigint,'row '||g FROM generate_series(1,20001) g`,
		`ANALYZE public.large_sparse`,
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
	transport := &http.Transport{MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1}
	t.Cleanup(transport.CloseIdleConnections)
	httpClient := &http.Client{Timeout: 10 * time.Second, Transport: transport}
	var latestConn net.Conn
	latestReused := false
	call := func(p pageRequest, token string) (int, map[string]any) {
		t.Helper()
		b, _ := json.Marshal(p)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/api/table/v2/page", bytes.NewReader(b))
		if err != nil {
			t.Fatal("request failed")
		}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { latestConn = info.Conn; latestReused = info.Reused }}))
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
	// Traverse real server-issued cursors into a large sparse relation. The
	// expected rows come from a separate native text projection, not a decoder
	// or cursor compiler shared with the route under test.
	t.Run("large sparse deep traversal", func(t *testing.T) {
		deep := pageRequest{ConnectionID: "native", Schema: "public", Table: "large_sparse", Profile: "postgres-direct", Limit: 1000}
		for page := 1; page < 20; page++ {
			status, result := call(deep, s.sessionToken)
			if status != 200 || len(keys(result)) != 1000 || result["hasNext"] != true {
				t.Fatalf("sparse page %d: status%d", page, status)
			}
			deep.Cursor = result["nextCursor"].(string)
		}
		const boundary int64 = 9007199254740993 + 19000*100003
		var oracle []string
		if err := fixture.QueryRow(ctx, `SELECT pg_catalog.array_agg(id::text ORDER BY id) FROM (SELECT id FROM public.large_sparse WHERE id>$1 ORDER BY id LIMIT 1000) expected`, boundary).Scan(&oracle); err != nil {
			t.Fatal("deep sparse native oracle failed")
		}
		status, result := call(deep, s.sessionToken)
		if status != 200 || strings.Join(keys(result), ",") != strings.Join(oracle, ",") || len(oracle) != 1000 || result["hasNext"] != true {
			t.Fatal("deep sparse keys differ from independent native oracle")
		}
		// Delete the cursor anchor and insert behind it: continuation still uses
		// its exact value, without looking up a surviving anchor or shifting an
		// offset into the replacement row set.
		if err := fixture.Exec(ctx, `DELETE FROM public.large_sparse WHERE id=$1`, boundary); err != nil {
			t.Fatal("deep sparse anchor deletion failed")
		}
		if err := fixture.Exec(ctx, `INSERT INTO public.large_sparse VALUES (1,'inserted behind boundary')`); err != nil {
			t.Fatal("deep sparse insertion failed")
		}
		if status, changed := call(deep, s.sessionToken); status != 200 || strings.Join(keys(changed), ",") != strings.Join(oracle, ",") {
			t.Fatal("deep sparse continuation shifted after behind-boundary writes")
		}
		last := deep
		last.Cursor = result["nextCursor"].(string)
		if status, tail := call(last, s.sessionToken); status != 200 || len(keys(tail)) != 1 || tail["hasNext"] != false || tail["nextCursor"] != "" {
			t.Fatal("deep sparse final page bounds incorrect")
		}
		// Native planner evidence for the profile's candidate-ID query. This
		// establishes bounded range traversal on this fixture, not a timing win
		// or a promise about every PostgreSQL data distribution.
		var explained string
		if err := fixture.QueryRow(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id FROM public.large_sparse WHERE id>$1 ORDER BY id ASC LIMIT 1001`, boundary).Scan(&explained); err != nil {
			t.Fatal("deep sparse native EXPLAIN failed")
		}
		var plans []struct{ Plan map[string]any }
		if err := json.Unmarshal([]byte(explained), &plans); err != nil || len(plans) != 1 {
			t.Fatal("deep sparse native plan unavailable")
		}
		var hasBoundedRange func(map[string]any) bool
		hasBoundedRange = func(plan map[string]any) bool {
			kind, _ := plan["Node Type"].(string)
			condition, _ := plan["Index Cond"].(string)
			rows, _ := plan["Actual Rows"].(float64)
			if (kind == "Index Scan" || kind == "Index Only Scan") && strings.Contains(condition, ">") && rows <= 1001 {
				return true
			}
			children, _ := plan["Plans"].([]any)
			for _, child := range children {
				if node, ok := child.(map[string]any); ok && hasBoundedRange(node) {
					return true
				}
			}
			return false
		}
		if !hasBoundedRange(plans[0].Plan) {
			t.Fatal("deep sparse candidate query did not use a bounded native index range")
		}
		t.Logf("deep sparse native candidate-ID plan: %s", explained)
	})
	// A real TCP keep-alive request after the old socket deadline must succeed
	// on the SAME connection, not silently reconnect around a leaked deadline.
	savedConn := latestConn
	time.Sleep(5100 * time.Millisecond)
	if status, _ = call(p, s.sessionToken); status != 200 || !latestReused || latestConn != savedConn {
		t.Fatal("page deadline leaked or keep-alive connection was not reused")
	}
	// An actual SELECT-only role can browse but must never be advertised writable.
	if err = admin.Exec(ctx, "CREATE ROLE "+quoteIdent(readerRole)+" NOLOGIN"); err != nil {
		t.Fatal("create owned reader role failed")
	}
	roleCreated = true
	for _, stmt := range []string{"GRANT USAGE ON SCHEMA \"Odd Schema\" TO " + quoteIdent(readerRole), "GRANT SELECT ON \"Odd Schema\".\"Odd Table\" TO " + quoteIdent(readerRole)} {
		if err = fixture.Exec(ctx, stmt); err != nil {
			t.Fatal("reader grant failed")
		}
	}
	readerURL := *fixtureURL
	readerOptions := readerURL.Query()
	readerOptions.Set("options", "-c search_path=public,pg_catalog -c role="+readerRole)
	readerURL.RawQuery = readerOptions.Encode()
	reader, err := db.Connect(ctx, readerURL.String())
	if err != nil {
		t.Fatal("reader connect failed")
	}
	t.Cleanup(reader.Close)
	s.mu.Lock()
	s.clients["reader"] = reader
	s.epochs["reader"] = "reader-epoch"
	s.mu.Unlock()
	readerPage := p
	readerPage.ConnectionID = "reader"
	if status, result := call(readerPage, s.sessionToken); status != 200 || result["readOnly"] != true || result["readOnlyReason"] == "" {
		t.Fatalf("SELECT-only role advertised writable: status%d %v", status, result)
	}
	if err = fixture.Exec(ctx, "REVOKE SELECT ON \"Odd Schema\".\"Odd Table\" FROM "+quoteIdent(readerRole)); err != nil {
		t.Fatal("revoke owned reader privilege failed")
	}
	if status, result := call(readerPage, s.sessionToken); status != 502 || result["state"] != nil {
		t.Fatal("permission failure was misclassified as unsupported profile")
	}
	// A blocked physical relation exercises the real 5s request deadline and
	// cancellation; a subsequent request must reuse the pool successfully.
	blocker, err := fixture.BeginTx(ctx)
	if err != nil {
		t.Fatal("blocker begin failed")
	}
	defer func() {
		cleanup, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = blocker.Rollback(cleanup)
	}()
	if _, err = blocker.Exec(ctx, `LOCK TABLE "Odd Schema"."Odd Table" IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal("blocker lock failed")
	}
	cancelCtx, cancelRequest := context.WithTimeout(ctx, 100*time.Millisecond)
	requestBody, _ := json.Marshal(p)
	cancelReq, _ := http.NewRequestWithContext(cancelCtx, http.MethodPost, ts.URL+"/api/table/v2/page", bytes.NewReader(requestBody))
	cancelReq.Header.Set(sessionHeader, s.sessionToken)
	cancelReq.Header.Set("Origin", ts.URL)
	canceledResponse, cancelErr := httpClient.Do(cancelReq)
	cancelRequest()
	if canceledResponse != nil {
		canceledResponse.Body.Close()
	}
	if cancelErr == nil {
		t.Fatal("canceled blocked request unexpectedly succeeded")
	}
	started := time.Now()
	if status, _ = call(p, s.sessionToken); status != 502 || time.Since(started) < 4*time.Second || time.Since(started) > 8*time.Second {
		t.Fatal("blocked relation did not honor bounded deadline")
	}
	if err = blocker.Rollback(ctx); err != nil {
		t.Fatal("blocker release failed")
	}
	if status, _ = call(p, s.sessionToken); status != 200 {
		t.Fatal("pool unusable after cancellation/deadline")
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
	if err = fixture.Exec(ctx, `ALTER TABLE "Odd Schema"."Odd Table" ADD COLUMN new_column pg_catalog.text`); err != nil {
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
	if err = fixture.Exec(ctx, `DROP TABLE "Odd Schema"."Odd Table";CREATE TABLE "Odd Schema"."Odd Table"("Key" bigint PRIMARY KEY,body pg_catalog.text)`); err != nil {
		t.Fatal("recreate failed")
	}
	p.Cursor = oldCursor
	if status, _ = call(p, s.sessionToken); status != 409 {
		t.Fatal("replacement relation cursor accepted")
	}
	for _, table := range []string{"int_key", "domain_key", "composite_key", "parent_key", "child_key", "json_precision", "json_text", "array_value", "custom_value"} {
		changed := pageRequest{ConnectionID: "native", Schema: "public", Table: table, Profile: "postgres-direct", Limit: 2}
		if status, result := call(changed, s.sessionToken); status != 400 || result["state"] != "unsupported-profile" {
			t.Fatalf("unsupported %s status%d/state%v", table, status, result["state"])
		}
	}
	if err = fixture.Exec(ctx, `CREATE TABLE public.huge(id bigint PRIMARY KEY, body pg_catalog.text);INSERT INTO public.huge VALUES(1,repeat('x',8388609))`); err != nil {
		t.Fatal("huge fixture failed")
	}
	huge := pageRequest{ConnectionID: "native", Schema: "public", Table: "huge", Profile: "postgres-direct", Limit: 1}
	if status, _ = call(huge, s.sessionToken); status != 413 {
		t.Fatalf("huge row status%d", status)
	}
}
