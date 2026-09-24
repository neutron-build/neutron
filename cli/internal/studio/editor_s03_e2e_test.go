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

// TestStudioDataEditorS03E2E walks the S03 data-editor journeys (V15 scope
// owned by S03) through the REAL route table and corsMiddleware against a
// REAL disposable Postgres database: the exact HTTP conversation the SPA
// performs (read -> stage -> preview -> commit -> revert), typed
// insert/delete with the NULL/DEFAULT wire discipline, multi-filter and
// multi-sort reads with separated row counts, composite FK navigation, and
// the two exit invariants as adversarial cases (no unrelated-row spillover;
// another connection's draft can never commit). Also pins the S02 review-F1
// entry condition: revert-time FK side effects abort the revert.
//
// The connection is hand-registered (the connection store persists to the
// user's config directory; a test must not mutate real user state). All
// state assertions use an independent SQL oracle on the fixture connection.
// Skipped unless NEUTRON_E2E_DATABASE_URL is set; NEUTRON_LIVE_REQUIRED=1
// turns a missing URL into a failure. Uses one uniquely-named s03_* database,
// dropped afterwards.
func TestStudioDataEditorS03E2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio data editor e2e skipped")
	}

	dbName := fmt.Sprintf("s03_%d_%d", os.Getpid(), time.Now().Unix())
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
		if !strings.HasPrefix(dbName, "s03_") {
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
	// editors exercises every typed shape S03 edits: tagged int8/numeric/
	// temporal columns, boolean, JSON, NOT NULL + DEFAULT columns.
	// parents/children exercise composite FK navigation. fkw exercises the
	// S02 review-F1 revert window (UNIQUE non-key column referenced ON
	// UPDATE CASCADE).
	for _, stmt := range []string{
		`CREATE TABLE editors (` +
			` id int PRIMARY KEY,` +
			` body text NOT NULL,` +
			` note text DEFAULT 'seed',` +
			` flag boolean NOT NULL DEFAULT false,` +
			` big bigint,` +
			` amount numeric(12,4),` +
			` seen_at timestamptz,` +
			` born_on date,` +
			` doc jsonb)`,
		`CREATE TABLE parents (tenant int, seq int, label text, PRIMARY KEY (tenant, seq))`,
		`CREATE TABLE children (` +
			` id int PRIMARY KEY,` +
			` p_tenant int, p_seq bigint,` +
			` FOREIGN KEY (p_tenant, p_seq) REFERENCES parents (tenant, seq))`,
		`CREATE TABLE fkw (id int PRIMARY KEY, pcode text UNIQUE)`,
		`CREATE TABLE fkw_children (id int PRIMARY KEY, parent_code text REFERENCES fkw (pcode) ON UPDATE CASCADE)`,
	} {
		if err := fixture.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Two hand-registered connections over DIFFERENT databases: the second
	// proves a draft staged through one connection can never commit through
	// the other (exit invariant 2).
	otherDB := dbName + "_other"
	if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE DATABASE %q`, otherDB)); err != nil {
		t.Fatalf("create database %s: %v", otherDB, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := admin.Exec(ctx, fmt.Sprintf(
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()`, otherDB,
		)); err != nil {
			t.Errorf("terminate backends: %v", err)
		}
		if !strings.HasPrefix(otherDB, "s03_") {
			t.Errorf("refusing to drop unexpected database %q", otherDB)
			return
		}
		if err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, otherDB)); err != nil {
			t.Errorf("drop database %s: %v", otherDB, err)
		}
	})
	other, err := db.Connect(context.Background(), deriveStudioDatabaseURL(t, base, otherDB))
	if err != nil {
		t.Fatalf("connect other: %v", err)
	}
	defer other.Close()
	if err := other.Exec(context.Background(),
		`CREATE TABLE editors (id int PRIMARY KEY, body text NOT NULL, note text DEFAULT 'seed', flag boolean NOT NULL DEFAULT false, big bigint, amount numeric(12,4), seen_at timestamptz, born_on date, doc jsonb)`); err != nil {
		t.Fatalf("seed other: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	s := &Server{
		port: port, sessionToken: fmt.Sprintf("s03-token-%d", port),
		clients: map[string]*db.Client{"e2e": fixture, "other": other},
	}
	mux, err := s.routes()
	if err != nil {
		t.Fatalf("routes: %v", err)
	}
	ts := httptest.NewUnstartedServer(s.corsMiddleware(mux))
	ts.Listener = ln
	ts.Start()
	defer ts.Close()
	token := s.sessionToken

	client := &http.Client{Timeout: 15 * time.Second}

	do := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rd)
		if err != nil {
			t.Fatalf("request %s %s: %v", method, path, err)
		}
		req.Header.Set("Origin", ts.URL)
		req.Header.Set(sessionHeader, token)
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
		return res.StatusCode, parsed
	}

	commit := func(connID, opID, payloadOps string) (int, map[string]any) {
		t.Helper()
		return do(http.MethodPost, "/api/table/v2/commit", fmt.Sprintf(
			`{"connectionId":%q,"operationId":%q,%s}`, connID, opID, payloadOps))
	}

	// oracle verifies state with SQL written independently of the handler
	// paths (the fixture connection, not the Studio API).
	oracle := func(query string, dest ...any) {
		t.Helper()
		if err := fixture.QueryRow(context.Background(), query).Scan(dest...); err != nil {
			t.Fatalf("oracle %q: %v", query, err)
		}
	}

	bindingOf := func(table string) string {
		t.Helper()
		code, body := do(http.MethodGet,
			"/api/table/v2/meta?connectionId=e2e&schema=public&table="+table, "")
		if code != http.StatusOK {
			t.Fatalf("meta %s: %d %v", table, code, body)
		}
		b, _ := body["binding"].(string)
		if b == "" {
			t.Fatalf("meta for %s carried no binding: %v", table, body)
		}
		return b
	}

	// xminOf reads a row's current version through the oracle.
	xminOf := func(table, keyCond string) string {
		t.Helper()
		var v string
		oracle(fmt.Sprintf(`SELECT xmin::text FROM %s WHERE %s`, table, keyCond), &v)
		return v
	}

	t.Run("S02-F1 regression: a child appearing between commit and revert aborts the revert, never cascades", func(t *testing.T) {
		binding := bindingOf("fkw")
		if err := fixture.Exec(context.Background(), `INSERT INTO fkw VALUES (1, 'A')`); err != nil {
			t.Fatalf("seed fkw: %v", err)
		}
		ver := xminOf("fkw", "id = 1")

		// Commit an update of the UNIQUE non-key column. No child exists
		// at commit time, so the batch is honestly reversible.
		ops := fmt.Sprintf(`"operations":[{"op":"update","schema":"public","table":"fkw","binding":%q,`+
			`"key":[{"column":"id","value":1}],"version":%q,"column":"pcode","value":"B"}]`, binding, ver)
		code, body := commit("e2e", "s03-f1-commit", ops)
		if code != http.StatusOK {
			t.Fatalf("commit = %d %v", code, body)
		}
		if body["reversible"] != true {
			t.Fatalf("commit must be reversible at commit time: %v", body)
		}

		// A child starts referencing the committed value AFTER the commit.
		if err := fixture.Exec(context.Background(), `INSERT INTO fkw_children VALUES (10, 'B')`); err != nil {
			t.Fatalf("seed child: %v", err)
		}

		// The revert must refuse: the inverse update would silently cascade
		// the child (pcode B -> A). Everything must stay unchanged.
		code, body = do(http.MethodPost, "/api/table/v2/revert",
			`{"connectionId":"e2e","operationId":"s03-f1-commit","revertOperationId":"s03-f1-revert"}`)
		if code != http.StatusConflict {
			t.Fatalf("revert with post-commit child = %d %v, want 409", code, body)
		}
		if state, _ := body["state"].(string); state != "irreversible" {
			t.Fatalf("revert refusal state = %q, want irreversible: %v", state, body)
		}
		var pcode, childCode string
		oracle(`SELECT pcode FROM fkw WHERE id = 1`, &pcode)
		oracle(`SELECT parent_code FROM fkw_children WHERE id = 10`, &childCode)
		if pcode != "B" || childCode != "B" {
			t.Fatalf("refused revert changed rows: pcode=%q child=%q, want B/B (unchanged)", pcode, childCode)
		}
	})

	t.Run("multi-filter reads AND conditions, validate columns, and separate counts", func(t *testing.T) {
		for _, stmt := range []string{
			`INSERT INTO editors (id, body, flag) VALUES (1, 'alpha', true), (2, 'beta', false), (3, 'gamma', true)`,
		} {
			if err := fixture.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		code, body := do(http.MethodGet, `/api/table?connectionId=e2e&schema=public&table=editors&limit=10`+
			`&filters=`+urlEncode(t, `[{"column":"flag","op":"eq","value":"true"},{"column":"body","op":"like","value":"%a%"}]`), "")
		if code != http.StatusOK {
			t.Fatalf("multi-filter read = %d %v", code, body)
		}
		rows := body["rows"].([]any)
		if len(rows) != 2 {
			t.Fatalf("flag=true AND body LIKE %%a%% matched %d rows, want 2 (alpha, gamma)", len(rows))
		}
		if fc := body["filterCount"].(json.Number).String(); fc != "2" {
			t.Fatalf("filterCount = %s, want 2", fc)
		}
		if tc := body["totalCount"].(json.Number).String(); tc != "3" {
			t.Fatalf("totalCount = %s, want 3", tc)
		}
		if rc := body["rowCount"].(json.Number).String(); rc != "2" {
			t.Fatalf("rowCount (fetched) = %s, want 2", rc)
		}

		// An unknown filter column is a 400 naming the column, not a SQL error envelope.
		code, body = do(http.MethodGet, `/api/table?connectionId=e2e&schema=public&table=editors&limit=10`+
			`&filters=`+urlEncode(t, `[{"column":"nope","op":"eq","value":"1"}]`), "")
		if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), `"nope"`) {
			t.Fatalf("unknown filter column = %d %v, want 400 naming the column", code, body)
		}
		// An unknown op is refused.
		code, body = do(http.MethodGet, `/api/table?connectionId=e2e&schema=public&table=editors&limit=10`+
			`&filters=`+urlEncode(t, `[{"column":"body","op":"regex","value":"x"}]`), "")
		if code != http.StatusBadRequest {
			t.Fatalf("unknown filter op = %d %v, want 400", code, body)
		}
	})

	t.Run("multi-sort applies keys in array order", func(t *testing.T) {
		for _, stmt := range []string{
			`INSERT INTO editors (id, body, flag) VALUES (4, 'beta', true), (5, 'alpha', false) ON CONFLICT DO NOTHING`,
		} {
			if err := fixture.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		code, body := do(http.MethodGet, `/api/table?connectionId=e2e&schema=public&table=editors&limit=10`+
			`&sorts=`+urlEncode(t, `[{"column":"body","dir":"asc"},{"column":"id","dir":"desc"}]`), "")
		if code != http.StatusOK {
			t.Fatalf("multi-sort read = %d %v", code, body)
		}
		rows := body["rows"].([]any)
		var bodies []string
		for _, r := range rows {
			row := r.([]any)
			bodies = append(bodies, row[1].(string))
		}
		// body asc, id desc: alpha(5), alpha(1), beta(4), beta(2), gamma(3).
		want := []string{"alpha", "alpha", "beta", "beta", "gamma"}
		if len(bodies) != len(want) {
			t.Fatalf("sorted bodies = %v, want %v", bodies, want)
		}
		for i := range want {
			if bodies[i] != want[i] {
				t.Fatalf("sorted bodies = %v, want %v", bodies, want)
			}
		}
		// Within the 'beta' block, id DESC puts 4 before 2.
		first := rows[2].([]any)
		second := rows[3].([]any)
		if first[0].(json.Number).String() != "4" || second[0].(json.Number).String() != "2" {
			t.Fatalf("secondary sort id DESC not applied: beta ids = %v, %v", first[0], second[0])
		}
		// Unknown sort column refused with 400.
		code, body = do(http.MethodGet, `/api/table?connectionId=e2e&schema=public&table=editors&limit=10`+
			`&sorts=`+urlEncode(t, `[{"column":"ghost","dir":"asc"}]`), "")
		if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), `"ghost"`) {
			t.Fatalf("unknown sort column = %d %v, want 400 naming the column", code, body)
		}
	})

	t.Run("typed insert journey: NULL, empty string and DEFAULT stay three distinct controls", func(t *testing.T) {
		binding := bindingOf("editors")
		// body='' (explicit empty string), note=null (SQL NULL), doc as JSON
		// text, tagged int8/numeric/temporal cells; flag/born_on omitted = DEFAULT.
		ops := `"operations":[{"op":"insert","schema":"public","table":"editors","binding":%q,"values":{` +
			`"id":10,"body":"","note":null,"big":{"t":"int8","v":"9007199254740993"},` +
			`"amount":{"t":"numeric","v":"12.3450"},"seen_at":{"t":"timestamptz","v":"2026-01-02T03:04:05.000001Z"},` +
			`"doc":"{\"a\":1}"}}]`
		code, body := commit("e2e", "s03-ins-1", fmt.Sprintf(ops, binding))
		if code != http.StatusOK {
			t.Fatalf("typed insert = %d %v", code, body)
		}
		var note *string
		var big string
		var amount string
		var seenOk bool
		var doc string
		var flag bool
		var bornOn *string
		oracle(`SELECT note, big::text, amount::text, seen_at = '2026-01-02T03:04:05.000001Z'::timestamptz, doc::text, flag, born_on::text FROM editors WHERE id = 10`,
			&note, &big, &amount, &seenOk, &doc, &flag, &bornOn)
		if note != nil {
			t.Fatal("note must be SQL NULL, not the 'seed' DEFAULT")
		}
		if big != "9007199254740993" {
			t.Fatalf("bigint insert rounded: %s", big)
		}
		if amount != "12.3450" {
			t.Fatalf("numeric insert lost scale: %s", amount)
		}
		if !seenOk {
			t.Fatal("timestamptz insert did not preserve microseconds")
		}
		if doc != `{"a": 1}` && doc != `{"a":1}` {
			t.Fatalf("jsonb insert: %s", doc)
		}
		if flag {
			t.Fatal("omitted flag must take its DEFAULT (false)")
		}
		if bornOn != nil {
			t.Fatal("omitted born_on must stay NULL (no default)")
		}

		// A staged insert previewed dry (nothing applied), then committed,
		// then reverted through the same conversation the SPA performs.
		ops = `"operations":[{"op":"insert","schema":"public","table":"editors","binding":%q,"values":{"id":11,"body":"revert-me"}}]`
		code, body = do(http.MethodPost, "/api/table/v2/preview",
			fmt.Sprintf(`{"connectionId":"e2e",`+ops+`}`, binding))
		if code != http.StatusOK || body["ok"] != true {
			t.Fatalf("insert preview = %d %v", code, body)
		}
		var n int
		oracle(`SELECT count(*) FROM editors WHERE id = 11`, &n)
		if n != 0 {
			t.Fatal("preview applied the insert")
		}
		code, body = commit("e2e", "s03-ins-2", fmt.Sprintf(ops, binding))
		if code != http.StatusOK {
			t.Fatalf("insert commit = %d %v", code, body)
		}
		code, body = do(http.MethodPost, "/api/table/v2/revert",
			`{"connectionId":"e2e","operationId":"s03-ins-2","revertOperationId":"s03-ins-2-r"}`)
		if code != http.StatusOK {
			t.Fatalf("insert revert = %d %v", code, body)
		}
		oracle(`SELECT count(*) FROM editors WHERE id = 11`, &n)
		if n != 0 {
			t.Fatal("revert left the inserted row behind")
		}
	})

	t.Run("typed update journey: boolean, JSON and tagged cells edit exactly", func(t *testing.T) {
		binding := bindingOf("editors")
		ver := xminOf("editors", "id = 10")
		ops := `"operations":[` +
			`{"op":"update","schema":"public","table":"editors","binding":%q,"key":[{"column":"id","value":10}],"version":%q,"column":"flag","value":false},` +
			`{"op":"update","schema":"public","table":"editors","binding":%q,"key":[{"column":"id","value":10}],"version":%q,"column":"doc","value":"{\"a\":2,\"b\":null}"}]`
		// The second update stages the SAME original version (the SPA stages
		// both from one read); the first update bumps xmin, so the batch
		// must conflict — proving per-op original rechecks also for the
		// editor's own earlier op.
		code, body := commit("e2e", "s03-upd-conflict", fmt.Sprintf(ops, binding, ver, binding, ver))
		if code != http.StatusConflict || body["state"] != "conflict" {
			t.Fatalf("second-op stale original = %d %v, want 409 conflict", code, body)
		}
		var flag bool
		var doc string
		oracle(`SELECT flag, doc::text FROM editors WHERE id = 10`, &flag, &doc)
		if flag != false || doc != `{"a": 1}` {
			t.Fatalf("conflicted batch applied something: flag=%v doc=%s", flag, doc)
		}

		// Re-read and commit the boolean edit on the fresh version — the
		// conversation the UI drives after a conflict refresh.
		ver = xminOf("editors", "id = 10")
		ops = `"operations":[{"op":"update","schema":"public","table":"editors","binding":%q,` +
			`"key":[{"column":"id","value":10}],"version":%q,"column":"flag","value":true}]`
		code, body = commit("e2e", "s03-upd-1", fmt.Sprintf(ops, binding, ver))
		if code != http.StatusOK {
			t.Fatalf("boolean update = %d %v", code, body)
		}
		ver = xminOf("editors", "id = 10")
		ops = `"operations":[{"op":"update","schema":"public","table":"editors","binding":%q,` +
			`"key":[{"column":"id","value":10}],"version":%q,"column":"doc","isNull":true}]`
		code, body = commit("e2e", "s03-upd-2", fmt.Sprintf(ops, binding, ver))
		if code != http.StatusOK {
			t.Fatalf("JSON null update = %d %v", code, body)
		}
		var docOut *string
		oracle(`SELECT flag, doc FROM editors WHERE id = 10`, &flag, &docOut)
		if flag != true || docOut != nil {
			t.Fatalf("typed updates: flag=%v doc=%v", flag, docOut)
		}

		// A malformed temporal edit is refused with column context.
		ver = xminOf("editors", "id = 10")
		ops = `"operations":[{"op":"update","schema":"public","table":"editors","binding":%q,` +
			`"key":[{"column":"id","value":10}],"version":%q,"column":"seen_at","value":{"t":"date","v":"2026-01-02"}}]`
		code, body = commit("e2e", "s03-upd-3", fmt.Sprintf(ops, binding, ver))
		if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), "does not match column type") {
			t.Fatalf("mismatched temporal tag = %d %v, want 400 tag mismatch", code, body)
		}
	})

	t.Run("delete journey: stage, preview dry, commit, revert re-inserts exactly", func(t *testing.T) {
		binding := bindingOf("editors")
		ver := xminOf("editors", "id = 1")
		ops := `"operations":[{"op":"delete","schema":"public","table":"editors","binding":%q,` +
			`"key":[{"column":"id","value":1}],"version":%q}]`
		code, body := do(http.MethodPost, "/api/table/v2/preview",
			fmt.Sprintf(`{"connectionId":"e2e",`+ops+`}`, binding, ver))
		if code != http.StatusOK {
			t.Fatalf("delete preview = %d %v", code, body)
		}
		beforeRow := body["operations"].([]any)[0].(map[string]any)["before"].(map[string]any)
		if beforeRow["body"] != "alpha" {
			t.Fatalf("delete preview before-row: %v", beforeRow)
		}
		var n int
		oracle(`SELECT count(*) FROM editors WHERE id = 1`, &n)
		if n != 1 {
			t.Fatal("preview deleted the row")
		}
		code, body = commit("e2e", "s03-del-1", fmt.Sprintf(ops, binding, ver))
		if code != http.StatusOK {
			t.Fatalf("delete commit = %d %v", code, body)
		}
		oracle(`SELECT count(*) FROM editors WHERE id = 1`, &n)
		if n != 0 {
			t.Fatal("delete not applied")
		}
		code, body = do(http.MethodPost, "/api/table/v2/revert",
			`{"connectionId":"e2e","operationId":"s03-del-1","revertOperationId":"s03-del-1-r"}`)
		if code != http.StatusOK {
			t.Fatalf("delete revert = %d %v", code, body)
		}
		var row string
		oracle(`SELECT row(id, body, note, flag)::text FROM editors WHERE id = 1`, &row)
		if row != `(1,alpha,seed,t)` {
			t.Fatalf("delete revert not exact: %s", row)
		}
	})

	t.Run("composite FK navigation lands on the referenced row by the whole tuple", func(t *testing.T) {
		if err := fixture.Exec(context.Background(),
			`INSERT INTO parents VALUES (7, 3, 'parent-a'), (7, 9, 'parent-b'), (8, 3, 'parent-c')`); err != nil {
			t.Fatalf("seed parents: %v", err)
		}
		if err := fixture.Exec(context.Background(),
			`INSERT INTO children VALUES (100, 7, 3)`); err != nil {
			t.Fatalf("seed children: %v", err)
		}

		// The SPA discovers the FK tuple from /table/fks.
		code, body := do(http.MethodGet,
			"/api/table/fks?connectionId=e2e&schema=public&table=children", "")
		if code != http.StatusOK {
			t.Fatalf("fks = %d %v", code, body)
		}
		fks := body["fks"].([]any)
		if len(fks) != 1 {
			t.Fatalf("children fks: %v", fks)
		}
		fk := fks[0].(map[string]any)
		if fk["refTable"] != "parents" || fk["composite"] != true {
			t.Fatalf("children fk shape: %v", fk)
		}
		concols := fk["columns"].([]any)
		refcols := fk["refColumns"].([]any)

		// Then reads the target with the full-tuple match. The child's
		// p_seq is bigint: the SPA sends the exact tagged cell.
		match := fmt.Sprintf(`[{"column":%q,"value":7},{"column":%q,"value":{"t":"int8","v":"3"}}]`,
			refcols[0].(string), refcols[1].(string))
		_ = concols
		code, body = do(http.MethodGet, `/api/table?connectionId=e2e&schema=public&table=parents&limit=10`+
			`&match=`+urlEncode(t, match), "")
		if code != http.StatusOK {
			t.Fatalf("match read = %d %v", code, body)
		}
		rows := body["rows"].([]any)
		if len(rows) != 1 {
			t.Fatalf("full-tuple match found %d rows, want exactly 1 (7,3)", len(rows))
		}
		row := rows[0].([]any)
		if row[0].(json.Number).String() != "7" || row[1].(json.Number).String() != "3" {
			t.Fatalf("matched the wrong row: %v", row)
		}
	})

	t.Run("exit invariant 1: a stale draft conflicts without touching any row, related or not", func(t *testing.T) {
		if err := fixture.Exec(context.Background(),
			`INSERT INTO editors (id, body) VALUES (20, 'r1'), (21, 'r2'), (22, 'r3')`); err != nil {
			t.Fatalf("seed: %v", err)
		}
		binding := bindingOf("editors")
		// Stage versions from a read...
		ver20 := xminOf("editors", "id = 20")
		ver21 := xminOf("editors", "id = 21")
		// ...then the rows change underneath the draft.
		if err := fixture.Exec(context.Background(), `UPDATE editors SET body = 'moved-on' WHERE id IN (20, 21)`); err != nil {
			t.Fatalf("external write: %v", err)
		}

		// The adversarial batch: a VALID edit of an unrelated row (22)
		// plus two STALE edits (20, 21). One edit must never change an
		// unrelated row: the whole batch refuses, nothing applies.
		ops := `"operations":[` +
			`{"op":"update","schema":"public","table":"editors","binding":%q,"key":[{"column":"id","value":22}],"version":%q,"column":"body","value":"innocent"},` +
			`{"op":"update","schema":"public","table":"editors","binding":%q,"key":[{"column":"id","value":20}],"version":%q,"column":"body","value":"stale-1"},` +
			`{"op":"update","schema":"public","table":"editors","binding":%q,"key":[{"column":"id","value":21}],"version":%q,"column":"body","value":"stale-2"}` +
			`]`
		code, body := commit("e2e", "s03-spill-1", fmt.Sprintf(ops,
			binding, xminOf("editors", "id = 22"),
			binding, ver20,
			binding, ver21))
		if code != http.StatusConflict || body["state"] != "conflict" {
			t.Fatalf("stale draft batch = %d %v, want 409 conflict", code, body)
		}
		var b20, b21, b22 string
		oracle(`SELECT body FROM editors WHERE id = 20`, &b20)
		oracle(`SELECT body FROM editors WHERE id = 21`, &b21)
		oracle(`SELECT body FROM editors WHERE id = 22`, &b22)
		if b20 != "moved-on" || b21 != "moved-on" {
			t.Fatalf("stale targets changed: %q %q", b20, b21)
		}
		if b22 != "r3" {
			t.Fatalf("unrelated row changed by a refused batch: %q", b22)
		}

		// The refused commit keeps its operation outcome honest: replaying
		// the same ID does not execute, and the draft stays reconcilable.
		code, body = do(http.MethodPost, "/api/table/v2/outcome",
			`{"connectionId":"e2e","operationId":"s03-spill-1"}`)
		if code != http.StatusOK || body["state"] != "failed" {
			t.Fatalf("outcome of refused batch = %d %v", code, body)
		}
	})

	t.Run("exit invariant 2: a draft staged through one connection cannot commit through another", func(t *testing.T) {
		if err := fixture.Exec(context.Background(),
			`INSERT INTO editors (id, body) VALUES (30, 'via-e2e')`); err != nil {
			t.Fatalf("seed: %v", err)
		}
		e2eBinding := bindingOf("editors")
		ver := xminOf("editors", "id = 30")

		// Adversarial: a commit REQUEST on connection "other" carrying
		// operations staged under connection "e2e" (e2e's binding, e2e's
		// rows). The server must refuse on the binding and apply nothing
		// on either database.
		ops := `"operations":[{"op":"update","schema":"public","table":"editors","binding":%q,` +
			`"key":[{"column":"id","value":30}],"version":%q,"column":"body","value":"smuggled"}]`
		code, body := commit("other", "s03-cross-1", fmt.Sprintf(ops, e2eBinding, ver))
		if code != http.StatusConflict || body["state"] != "binding" {
			t.Fatalf("cross-connection draft = %d %v, want 409 binding", code, body)
		}
		var body30 string
		oracle(`SELECT body FROM editors WHERE id = 30`, &body30)
		if body30 != "via-e2e" {
			t.Fatalf("cross-connection commit changed the row: %q", body30)
		}
		var otherN int
		if err := other.QueryRow(context.Background(), `SELECT count(*) FROM editors`).Scan(&otherN); err != nil {
			t.Fatalf("oracle other: %v", err)
		}
		if otherN != 0 {
			t.Fatalf("the other connection's database received rows: %d", otherN)
		}

		// The smuggled operation ID is scoped to the connection it was
		// sent on; the legitimate connection never saw it.
		code, body = do(http.MethodPost, "/api/table/v2/outcome",
			`{"connectionId":"e2e","operationId":"s03-cross-1"}`)
		if code != http.StatusOK || body["state"] != "unknown" {
			t.Fatalf("cross-connection outcome lookup = %d %v, want 200 unknown", code, body)
		}
	})
}

// urlEncode percent-encodes a query component exactly like URLSearchParams.
func urlEncode(t *testing.T, raw string) string {
	t.Helper()
	var out strings.Builder
	for _, r := range []byte(raw) {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.', r == '~':
			out.WriteByte(r)
		default:
			fmt.Fprintf(&out, "%%%02X", r)
		}
	}
	return out.String()
}
