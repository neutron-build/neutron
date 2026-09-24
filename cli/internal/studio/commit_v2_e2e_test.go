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

// TestStudioCommitProtocolV2E2E walks the S02 commit/retry journeys (V15
// scope owned by S02) through the REAL route table and corsMiddleware
// against a REAL disposable Postgres database: staging commits as ONE
// atomic unit with real late failures, operation-ID deduplication and
// outcome records, drop-response-after-commit resolution, expired/evicted
// records reporting unknown, preview/revert round-trips with irreversible
// refusals, body/operation limits and malformed tagged values, and the S01
// review-F1 regression (read-only reason before binding on no-key tables).
//
// The connection is hand-registered (the connection store persists to the
// user's config directory; a test must not mutate real user state). All
// state assertions use an independent SQL oracle on the fixture
// connection. Skipped unless NEUTRON_E2E_DATABASE_URL is set;
// NEUTRON_LIVE_REQUIRED=1 turns a missing URL into a failure. Uses one
// uniquely-named s02_* database, dropped afterwards.
func TestStudioCommitProtocolV2E2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio commit e2e skipped")
	}

	dbName := fmt.Sprintf("s02_%d_%d", os.Getpid(), time.Now().Unix())
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
		if !strings.HasPrefix(dbName, "s02_") {
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
		`CREATE TABLE commits (id int PRIMARY KEY, body text NOT NULL, note text DEFAULT 'seed', big bigint)`,
		`CREATE TABLE children (id int PRIMARY KEY, parent_id int REFERENCES commits(id), label text NOT NULL)`,
		`CREATE TABLE cascade_children (id int PRIMARY KEY, parent_id int REFERENCES commits(id) ON DELETE CASCADE, label text NOT NULL)`,
		`CREATE TABLE ident_rows (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text NOT NULL)`,
		`CREATE VIEW commits_v AS SELECT id, body FROM commits`,
		`CREATE TABLE nokey (a int, b text)`,
	} {
		if err := fixture.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Two servers over the same fixture connection: the main one with
	// generous retention, plus a short-retention instance whose endpoints
	// prove expiry/eviction honestly report unknown.
	startServer := func(store *outcomeStore) (*httptest.Server, string) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		s := &Server{
			port: port, sessionToken: fmt.Sprintf("s02-token-%d", port),
			clients: map[string]*db.Client{"e2e": fixture}, outcomes: store,
		}
		mux, err := s.routes()
		if err != nil {
			t.Fatalf("routes: %v", err)
		}
		ts := httptest.NewUnstartedServer(s.corsMiddleware(mux))
		ts.Listener = ln
		ts.Start()
		t.Cleanup(ts.Close)
		return ts, s.sessionToken
	}
	mainTS, mainToken := startServer(newOutcomeStore(64, time.Hour, time.Minute))
	evictTS, evictToken := startServer(newOutcomeStore(1, time.Hour, time.Minute))
	ttlTS, ttlToken := startServer(newOutcomeStore(64, 100*time.Millisecond, time.Minute))

	client := &http.Client{Timeout: 15 * time.Second}

	do := func(ts *httptest.Server, token, method, path, body string) (int, map[string]any) {
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
		return res.StatusCode, parsed
	}

	commit := func(ts *httptest.Server, token, connID, opID, payloadOps string) (int, map[string]any) {
		t.Helper()
		return do(ts, token, http.MethodPost, "/api/table/v2/commit", fmt.Sprintf(
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

	var binding string

	// bindingOf fetches a relation's live binding the way the SPA does.
	bindingOf := func(table string) string {
		t.Helper()
		code, body := do(mainTS, mainToken, http.MethodGet,
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

	readIdentities := func(table string) (versions map[string]string) {
		t.Helper()
		code, body := do(mainTS, mainToken, http.MethodGet,
			"/api/table?connectionId=e2e&schema=public&table="+table, "")
		if code != http.StatusOK {
			t.Fatalf("read %s: %d %v", table, code, body)
		}
		b, _ := body["binding"].(string)
		binding = b
		cols := body["columns"].([]any)
		rows := body["rows"].([]any)
		vers := body["versions"].([]any)
		idIdx := -1
		for i, c := range cols {
			if c == "id" {
				idIdx = i
			}
		}
		versions = map[string]string{}
		for i, r := range rows {
			row := r.([]any)
			id, err := row[idIdx].(json.Number).Int64()
			if err != nil {
				t.Fatalf("row %d id: %v", i, err)
			}
			v, _ := vers[i].(string)
			versions[fmt.Sprintf("%d", id)] = v
		}
		return versions
	}

	t.Run("a late FK failure rolls the whole batch back, FK consequences included", func(t *testing.T) {
		versions := readIdentities("commits")
		_ = versions
		commitsBinding, childrenBinding := bindingOf("commits"), bindingOf("children")
		ops := `"operations":[` +
			`{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":1,"body":"one"}},` +
			`{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":2,"body":"two"}},` +
			`{"op":"insert","schema":"public","table":"children","binding":%q,"values":{"id":10,"parent_id":999,"label":"orphan"}}` +
			`]`
		code, body := commit(mainTS, mainToken, "e2e", "op-fk-fail", fmt.Sprintf(ops, commitsBinding, commitsBinding, childrenBinding))
		if code != http.StatusConflict || body["state"] != "constraint" {
			t.Fatalf("late FK failure = %d %v, want 409 constraint", code, body)
		}
		var n int
		oracle(`SELECT count(*) FROM commits`, &n)
		if n != 0 {
			t.Fatalf("commits has %d rows after a failed batch, want 0 (all-or-nothing)", n)
		}
		oracle(`SELECT count(*) FROM children`, &n)
		if n != 0 {
			t.Fatalf("children has %d rows, want 0", n)
		}
	})

	t.Run("a committed batch is one unit with per-op identities", func(t *testing.T) {
		ops := `"operations":[` +
			`{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":1,"body":"one","note":"first"}},` +
			`{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":2,"body":"two","note":null}}` +
			`]`
		code, body := commit(mainTS, mainToken, "e2e", "op-good-1", fmt.Sprintf(ops, binding, binding))
		if code != http.StatusOK {
			t.Fatalf("commit = %d %v", code, body)
		}
		ra, ok := body["rowsAffected"].(json.Number)
		if !ok || ra.String() != "2" {
			t.Fatalf("rowsAffected = %v, want 2", body["rowsAffected"])
		}
		results := body["operations"].([]any)
		if len(results) != 2 {
			t.Fatalf("per-op results: %v", results)
		}
		first := results[0].(map[string]any)
		if v, _ := first["version"].(string); v == "" {
			t.Fatalf("insert result carries no row version: %v", first)
		}
		if key := first["key"].([]any); len(key) != 1 || key[0].(map[string]any)["column"] != "id" {
			t.Fatalf("insert result key: %v", key)
		}
		var null2 bool
		oracle(`SELECT note IS NULL FROM commits WHERE id = 2`, &null2)
		if !null2 {
			t.Fatal("JSON null in a batch insert must write SQL NULL")
		}
	})

	t.Run("duplicate operation ID with a different payload is rejected", func(t *testing.T) {
		ops := `"operations":[{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":3,"body":"three"}}]`
		code, body := commit(mainTS, mainToken, "e2e", "op-good-1", fmt.Sprintf(ops, binding))
		if code != http.StatusConflict || body["state"] != "operation_conflict" {
			t.Fatalf("different payload = %d %v, want 409 operation_conflict", code, body)
		}
		var n int
		oracle(`SELECT count(*) FROM commits WHERE id = 3`, &n)
		if n != 0 {
			t.Fatal("conflicting duplicate was applied")
		}
	})

	t.Run("duplicate operation ID with the same payload replays without re-executing", func(t *testing.T) {
		ops := `"operations":[` +
			`{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":1,"body":"one","note":"first"}},` +
			`{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":2,"body":"two","note":null}}` +
			`]`
		code, body := commit(mainTS, mainToken, "e2e", "op-good-1", fmt.Sprintf(ops, binding, binding))
		if code != http.StatusOK {
			t.Fatalf("replay = %d %v", code, body)
		}
		if body["replayed"] != true {
			t.Fatalf("replay marker missing: %v", body)
		}
		var n int
		oracle(`SELECT count(*) FROM commits`, &n)
		if n != 2 {
			t.Fatalf("replayed insert re-executed: %d rows, want 2", n)
		}
	})

	t.Run("drop the response after commit, then resolve the outcome before retrying", func(t *testing.T) {
		versions := readIdentities("commits")
		ops := `"operations":[{"op":"update","schema":"public","table":"commits","binding":%q,` +
			`"key":[{"column":"id","value":1}],"version":%q,"column":"note","value":"dropped-response"}]`
		payload := fmt.Sprintf(ops, binding, versions["1"])

		// The commit happens; the client loses the response (network drop
		// after the server wrote its outcome). Only the status is observed.
		code, dropped := commit(mainTS, mainToken, "e2e", "op-drop-1", payload)
		if code != http.StatusOK {
			t.Fatalf("dropped commit = %d %v", code, dropped)
		}
		firstVersion, _ := dropped["operations"].([]any)[0].(map[string]any)["version"].(string)
		var xmin1 string
		oracle(`SELECT xmin::text FROM commits WHERE id = 1`, &xmin1)

		// Status lookup must precede retry: the outcome endpoint resolves
		// what happened without touching the table.
		code, outcome := do(mainTS, mainToken, http.MethodPost, "/api/table/v2/outcome",
			`{"connectionId":"e2e","operationId":"op-drop-1"}`)
		if code != http.StatusOK || outcome["state"] != "committed" {
			t.Fatalf("outcome lookup = %d %v", code, outcome)
		}

		// The retry with the SAME operation ID replays the recorded
		// outcome; the update is NOT executed a second time.
		code, retry := commit(mainTS, mainToken, "e2e", "op-drop-1", payload)
		if code != http.StatusOK || retry["replayed"] != true {
			t.Fatalf("retry after drop = %d %v", code, retry)
		}
		retryVersion, _ := retry["operations"].([]any)[0].(map[string]any)["version"].(string)
		if retryVersion != firstVersion {
			t.Fatalf("replayed version %q != recorded %q", retryVersion, firstVersion)
		}
		var note string
		var xmin2 string
		oracle(`SELECT note FROM commits WHERE id = 1`, &note)
		oracle(`SELECT xmin::text FROM commits WHERE id = 1`, &xmin2)
		if note != "dropped-response" {
			t.Fatalf("note = %q", note)
		}
		if xmin1 != xmin2 {
			t.Fatalf("retry re-executed the update (xmin %q -> %q)", xmin1, xmin2)
		}
	})

	t.Run("unknown operation IDs report unknown, never a guess", func(t *testing.T) {
		code, body := do(mainTS, mainToken, http.MethodPost, "/api/table/v2/outcome",
			`{"connectionId":"e2e","operationId":"never-used"}`)
		if code != http.StatusOK || body["state"] != "unknown" {
			t.Fatalf("never-seen outcome = %d %v, want 200 unknown", code, body)
		}
	})

	t.Run("expired outcome records report unknown and refuse identical retries", func(t *testing.T) {
		readIdentities("commits") // both servers share the hand-built epoch "0"
		updateOps := func(version, value string) string {
			return fmt.Sprintf(`"operations":[{"op":"update","schema":"public","table":"commits","binding":%q,`+
				`"key":[{"column":"id","value":2}],"version":%q,"column":"note","value":%q}]`, binding, version, value)
		}

		// Commit on the short-TTL server, then let the record expire.
		payload := updateOps(currentXmin(t, oracle, 2), "ttl-a")
		code, body := commit(ttlTS, ttlToken, "e2e", "op-ttl-1", payload)
		if code != http.StatusOK {
			t.Fatalf("ttl commit = %d %v", code, body)
		}
		time.Sleep(200 * time.Millisecond)
		code, body = do(ttlTS, ttlToken, http.MethodPost, "/api/table/v2/outcome",
			`{"connectionId":"e2e","operationId":"op-ttl-1"}`)
		if code != http.StatusOK || body["state"] != "unknown" {
			t.Fatalf("expired outcome = %d %v, want 200 unknown", code, body)
		}
		// Retrying the IDENTICAL payload is refused as unknown: the
		// expired outcome is never guessed and never safe-to-repeat.
		code, body = commit(ttlTS, ttlToken, "e2e", "op-ttl-1", payload)
		if code != http.StatusConflict || body["state"] != "unknown" {
			t.Fatalf("expired retry = %d %v, want 409 unknown", code, body)
		}
		var note string
		oracle(`SELECT note FROM commits WHERE id = 2`, &note)
		if note != "ttl-a" {
			t.Fatalf("note = %q, want ttl-a — the refused retry must not have executed", note)
		}
	})

	t.Run("evicted outcome records report unknown and refuse retries", func(t *testing.T) {
		updateOps := func(version, value string) string {
			return fmt.Sprintf(`"operations":[{"op":"update","schema":"public","table":"commits","binding":%q,`+
				`"key":[{"column":"id","value":2}],"version":%q,"column":"note","value":%q}]`, binding, version, value)
		}
		payloadA := updateOps(currentXmin(t, oracle, 2), "evict-a")
		code, body := commit(evictTS, evictToken, "e2e", "op-evict-1", payloadA)
		if code != http.StatusOK {
			t.Fatalf("capacity-first commit = %d %v", code, body)
		}
		// The capacity-1 store evicts the first record to make room; its
		// tombstone keeps the ID honest.
		code, body = commit(evictTS, evictToken, "e2e", "op-evict-2", updateOps(currentXmin(t, oracle, 2), "evict-b"))
		if code != http.StatusOK {
			t.Fatalf("capacity-second commit = %d %v", code, body)
		}
		code, body = do(evictTS, evictToken, http.MethodPost, "/api/table/v2/outcome",
			`{"connectionId":"e2e","operationId":"op-evict-2"}`)
		if code != http.StatusOK || body["state"] != "committed" {
			t.Fatalf("retained outcome = %d %v", code, body)
		}
		code, body = do(evictTS, evictToken, http.MethodPost, "/api/table/v2/outcome",
			`{"connectionId":"e2e","operationId":"op-evict-1"}`)
		if code != http.StatusOK || body["state"] != "unknown" {
			t.Fatalf("evicted outcome = %d %v, want 200 unknown", code, body)
		}
		code, body = commit(evictTS, evictToken, "e2e", "op-evict-1", payloadA)
		if code != http.StatusConflict || body["state"] != "unknown" {
			t.Fatalf("evicted retry = %d %v, want 409 unknown", code, body)
		}
	})

	t.Run("two clients committing the same operation ID: one execution, both get the outcome", func(t *testing.T) {
		ops := `"operations":[{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":77,"body":"race"}}]`
		payload := fmt.Sprintf(`{"connectionId":"e2e","operationId":"op-race-1",`+ops+`}`, binding)

		post := func() (int, map[string]any) {
			req, err := http.NewRequest(http.MethodPost, mainTS.URL+"/api/table/v2/commit", strings.NewReader(payload))
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			req.Header.Set("Origin", mainTS.URL)
			req.Header.Set(sessionHeader, mainToken)
			req.Header.Set("Content-Type", "application/json")
			res, err := client.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer res.Body.Close()
			raw, _ := io.ReadAll(res.Body)
			var parsed map[string]any
			dec := json.NewDecoder(strings.NewReader(string(raw)))
			dec.UseNumber()
			if err := dec.Decode(&parsed); err != nil {
				t.Fatalf("decode %q: %v", raw, err)
			}
			return res.StatusCode, parsed
		}

		start := make(chan struct{})
		resCh := make(chan [2]any, 2)
		for i := 0; i < 2; i++ {
			go func() {
				<-start
				code, body := post()
				resCh <- [2]any{code, body}
			}()
		}
		close(start)

		sawOriginal, sawReplayOrInProgress := false, false
		for i := 0; i < 2; i++ {
			res := <-resCh
			code := res[0].(int)
			body := res[1].(map[string]any)
			switch {
			case code == http.StatusOK && body["replayed"] == true:
				sawReplayOrInProgress = true
			case code == http.StatusOK:
				sawOriginal = true
			case code == http.StatusConflict && body["state"] == "in_progress":
				sawReplayOrInProgress = true
				// A concurrent duplicate may retry the same payload and
				// then must receive the committed outcome.
				for retry := 0; retry < 20; retry++ {
					code2, body2 := post()
					if code2 == http.StatusOK {
						if body2["replayed"] != true {
							t.Fatalf("in-progress retry re-executed: %v", body2)
						}
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
			default:
				t.Fatalf("concurrent duplicate got %d %v", code, body)
			}
		}
		if !sawOriginal || !sawReplayOrInProgress {
			t.Fatalf("expected one original execution and one replay/in-progress; original=%v replay=%v", sawOriginal, sawReplayOrInProgress)
		}
		var n int
		oracle(`SELECT count(*) FROM commits WHERE id = 77`, &n)
		if n != 1 {
			t.Fatalf("race produced %d rows, want exactly 1 (no duplicated insert)", n)
		}
	})

	t.Run("preview is a dry run: the exact diff, nothing applied", func(t *testing.T) {
		versions := readIdentities("commits")
		ops := `"operations":[` +
			`{"op":"update","schema":"public","table":"commits","binding":%q,"key":[{"column":"id","value":1}],"version":%q,"column":"note","value":"previewed"},` +
			`{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":99,"body":"preview-only"}},` +
			`{"op":"delete","schema":"public","table":"commits","binding":%q,"key":[{"column":"id","value":2}],"version":%q}` +
			`]`
		code, body := do(mainTS, mainToken, http.MethodPost, "/api/table/v2/preview", fmt.Sprintf(
			`{"connectionId":"e2e",`+ops+`}`, binding, versions["1"], binding, binding, versions["2"]))
		if code != http.StatusOK || body["ok"] != true {
			t.Fatalf("preview = %d %v", code, body)
		}
		counts := body["counts"].(map[string]any)
		if counts["update"].(json.Number).String() != "1" || counts["insert"].(json.Number).String() != "1" || counts["delete"].(json.Number).String() != "1" {
			t.Fatalf("preview counts: %v", counts)
		}
		entries := body["operations"].([]any)
		upd := entries[0].(map[string]any)
		if upd["before"] != "dropped-response" || upd["after"] != "previewed" || upd["column"] != "note" {
			t.Fatalf("update diff: %v", upd)
		}
		ins := entries[1].(map[string]any)
		if ins["key"].([]any)[0].(map[string]any)["value"].(json.Number).String() != "99" {
			t.Fatalf("insert preview key (would-be identity): %v", ins)
		}
		del := entries[2].(map[string]any)
		beforeRow := del["before"].(map[string]any)
		if beforeRow["body"] != "two" {
			t.Fatalf("delete preview before-row: %v", beforeRow)
		}

		var n int
		var note string
		oracle(`SELECT count(*) FROM commits`, &n)
		oracle(`SELECT note FROM commits WHERE id = 1`, &note)
		if n != 3 || note != "dropped-response" {
			t.Fatalf("preview applied changes: n=%d note=%q", n, note)
		}
		oracle(`SELECT count(*) FROM commits WHERE id = 99`, &n)
		if n != 0 {
			t.Fatal("preview inserted its row")
		}
		oracle(`SELECT count(*) FROM commits WHERE id = 2`, &n)
		if n != 1 {
			t.Fatal("preview deleted a row")
		}
	})

	t.Run("a stale original anywhere in the batch conflicts the whole commit", func(t *testing.T) {
		versions := readIdentities("commits")
		// Someone else writes row 1 out from under the staged batch.
		if err := fixture.Exec(context.Background(), `UPDATE commits SET note = 'extern' WHERE id = 1`); err != nil {
			t.Fatalf("external write: %v", err)
		}
		var current string
		oracle(`SELECT xmin::text FROM commits WHERE id = 1`, &current)

		ops := `"operations":[` +
			`{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":55,"body":"should-not-exist"}},` +
			`{"op":"update","schema":"public","table":"commits","binding":%q,"key":[{"column":"id","value":1}],"version":%q,"column":"note","value":"stale"}` +
			`]`
		code, body := commit(mainTS, mainToken, "e2e", "op-stale-1", fmt.Sprintf(ops, binding, binding, versions["1"]))
		if code != http.StatusConflict || body["state"] != "conflict" {
			t.Fatalf("stale-in-batch = %d %v, want 409 conflict", code, body)
		}
		if cv, _ := body["currentVersion"].(string); cv != current {
			t.Fatalf("currentVersion %q != actual xmin %q", cv, current)
		}
		var n int
		oracle(`SELECT count(*) FROM commits WHERE id = 55`, &n)
		if n != 0 {
			t.Fatal("the earlier batch operation was applied before the stale one failed")
		}
	})

	t.Run("a second edit of the same row inside one batch rechecks its original", func(t *testing.T) {
		versions := readIdentities("commits")
		ops := `"operations":[` +
			`{"op":"update","schema":"public","table":"commits","binding":%q,"key":[{"column":"id","value":1}],"version":%q,"column":"note","value":"first-edit"},` +
			`{"op":"update","schema":"public","table":"commits","binding":%q,"key":[{"column":"id","value":1}],"version":%q,"column":"note","value":"second-edit"}` +
			`]`
		code, body := commit(mainTS, mainToken, "e2e", "op-selfconflict-1", fmt.Sprintf(ops, binding, versions["1"], binding, versions["1"]))
		if code != http.StatusConflict || body["state"] != "conflict" {
			t.Fatalf("self-conflict = %d %v, want 409 conflict (the second edit's original is gone)", code, body)
		}
		var note string
		oracle(`SELECT note FROM commits WHERE id = 1`, &note)
		if note != "extern" {
			t.Fatalf("note = %q, want extern (nothing applied)", note)
		}
	})

	t.Run("revert restores the exact pre-commit state through the recorded inverse", func(t *testing.T) {
		versions := readIdentities("commits")
		var row2Before string
		oracle(`SELECT row(id, body, note, big)::text FROM commits WHERE id = 2`, &row2Before)

		ops := `"operations":[` +
			`{"op":"update","schema":"public","table":"commits","binding":%q,"key":[{"column":"id","value":1}],"version":%q,"column":"note","value":"revert-me"},` +
			`{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":50,"body":"born-to-die"}},` +
			`{"op":"delete","schema":"public","table":"commits","binding":%q,"key":[{"column":"id","value":2}],"version":%q}` +
			`]`
		code, body := commit(mainTS, mainToken, "e2e", "op-revert-1", fmt.Sprintf(ops, binding, versions["1"], binding, binding, versions["2"]))
		if code != http.StatusOK {
			t.Fatalf("commit = %d %v", code, body)
		}
		if body["reversible"] != true {
			t.Fatalf("commit must be reversible: %v", body)
		}

		code, body = do(mainTS, mainToken, http.MethodPost, "/api/table/v2/revert",
			`{"connectionId":"e2e","operationId":"op-revert-1","revertOperationId":"op-revert-1-r"}`)
		if code != http.StatusOK {
			t.Fatalf("revert = %d %v", code, body)
		}
		if body["reverted"] != "op-revert-1" {
			t.Fatalf("revert response: %v", body)
		}

		var note string
		var n int
		var row2After string
		oracle(`SELECT note FROM commits WHERE id = 1`, &note)
		if note != "extern" {
			t.Fatalf("update not undone: note = %q, want extern", note)
		}
		oracle(`SELECT count(*) FROM commits WHERE id = 50`, &n)
		if n != 0 {
			t.Fatal("insert not undone")
		}
		oracle(`SELECT row(id, body, note, big)::text FROM commits WHERE id = 2`, &row2After)
		if row2After != row2Before {
			t.Fatalf("delete not undone exactly: %q vs %q", row2After, row2Before)
		}

		// The revert is deduplicated like any commit.
		code, body = do(mainTS, mainToken, http.MethodPost, "/api/table/v2/revert",
			`{"connectionId":"e2e","operationId":"op-revert-1","revertOperationId":"op-revert-1-r"}`)
		if code != http.StatusOK || body["replayed"] != true {
			t.Fatalf("revert replay = %d %v", code, body)
		}
	})

	t.Run("deleting a row with an identity key is honestly irreversible", func(t *testing.T) {
		if err := fixture.Exec(context.Background(), `INSERT INTO ident_rows (v) VALUES ('gone-soon')`); err != nil {
			t.Fatalf("seed ident: %v", err)
		}
		var id string
		var ver string
		oracle(`SELECT id::text, xmin::text FROM ident_rows`, &id, &ver)
		identBinding := bindingOf("ident_rows")

		ops := `"operations":[{"op":"delete","schema":"public","table":"ident_rows","binding":%q,` +
			`"key":[{"column":"id","value":%s}],"version":%q}]`
		code, body := commit(mainTS, mainToken, "e2e", "op-ident-1", fmt.Sprintf(ops, identBinding, id, ver))
		if code != http.StatusOK {
			t.Fatalf("commit = %d %v", code, body)
		}
		if body["reversible"] != false {
			t.Fatalf("identity-key delete must be irreversible: %v", body)
		}
		reason, _ := body["reversibleReason"].(string)
		if !strings.Contains(reason, "id") || !strings.Contains(reason, "identity") {
			t.Fatalf("irreversible reason must name the identity key: %q", reason)
		}
		code, body = do(mainTS, mainToken, http.MethodPost, "/api/table/v2/revert",
			`{"connectionId":"e2e","operationId":"op-ident-1","revertOperationId":"op-ident-1-r"}`)
		if code != http.StatusConflict || body["state"] != "irreversible" {
			t.Fatalf("revert of irreversible = %d %v, want 409 irreversible", code, body)
		}
	})

	t.Run("cascade FK side effects are applied atomically and flagged irreversible", func(t *testing.T) {
		if err := fixture.Exec(context.Background(),
			`INSERT INTO cascade_children VALUES (1, 1, 'child')`); err != nil {
			t.Fatalf("seed cascade child: %v", err)
		}
		versions := readIdentities("commits")
		ops := `"operations":[{"op":"delete","schema":"public","table":"commits","binding":%q,` +
			`"key":[{"column":"id","value":1}],"version":%q}]`
		code, body := commit(mainTS, mainToken, "e2e", "op-cascade-1", fmt.Sprintf(ops, binding, versions["1"]))
		if code != http.StatusOK {
			t.Fatalf("commit = %d %v", code, body)
		}
		if body["reversible"] != false {
			t.Fatalf("cascade-affected delete must be irreversible: %v", body)
		}
		reason, _ := body["reversibleReason"].(string)
		if !strings.Contains(reason, "ON DELETE") && !strings.Contains(reason, "cascade") {
			t.Fatalf("irreversible reason must name the FK rule: %q", reason)
		}
		var n int
		oracle(`SELECT count(*) FROM commits WHERE id = 1`, &n)
		if n != 0 {
			t.Fatal("delete not applied")
		}
		oracle(`SELECT count(*) FROM cascade_children`, &n)
		if n != 0 {
			t.Fatal("the FK cascade consequence must be applied with the commit")
		}
		code, body = do(mainTS, mainToken, http.MethodPost, "/api/table/v2/revert",
			`{"connectionId":"e2e","operationId":"op-cascade-1","revertOperationId":"op-cascade-1-r"}`)
		if code != http.StatusConflict || body["state"] != "irreversible" {
			t.Fatalf("revert of cascade delete = %d %v", code, body)
		}
	})

	t.Run("batch and body limits plus malformed tagged values are refused", func(t *testing.T) {
		// Operation-count limit.
		var sb strings.Builder
		sb.WriteString(`"operations":[`)
		for i := 0; i < 101; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(fmt.Sprintf(`{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":%d,"body":"x"}}`, binding, 1000+i))
		}
		sb.WriteString(`]`)
		code, body := commit(mainTS, mainToken, "e2e", "op-limit-count", sb.String())
		if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), "limited to 100 operations") {
			t.Fatalf("op-count limit = %d %v", code, body)
		}

		// Body-size limit (1 MiB default).
		big := strings.Repeat("A", 1<<20)
		code, body = commit(mainTS, mainToken, "e2e", "op-limit-body",
			fmt.Sprintf(`"operations":[{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":1100,"body":%q}}]`, binding, big))
		if code != http.StatusBadRequest {
			t.Fatalf("body-size limit = %d %v, want 400", code, body)
		}

		// Malformed tagged values keep the S01 wire discipline.
		code, body = commit(mainTS, mainToken, "e2e", "op-wire-1",
			fmt.Sprintf(`"operations":[{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":1200,"body":"x","big":9007199254740993}}]`, binding))
		if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), "wire cell") {
			t.Fatalf("bare int8 number = %d %v, want 400 naming the wire cell", code, body)
		}
		code, body = commit(mainTS, mainToken, "e2e", "op-wire-2",
			fmt.Sprintf(`"operations":[{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":1201,"body":"x","big":{"t":"numeric","v":"1"}}}]`, binding))
		if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), "does not match column type") {
			t.Fatalf("mismatched tag = %d %v", code, body)
		}

		// Unknown operation fields are refused by the strict decoder.
		code, body = commit(mainTS, mainToken, "e2e", "op-wire-3",
			fmt.Sprintf(`"operations":[{"op":"insert","schema":"public","table":"commits","binding":%q,"model":"kv","values":{"id":1202,"body":"x"}}]`, binding))
		if code != http.StatusBadRequest {
			t.Fatalf("unknown op field = %d %v, want 400", code, body)
		}

		var n int
		oracle(`SELECT count(*) FROM commits WHERE id >= 1000`, &n)
		if n != 0 {
			t.Fatal("a refused limits/wire batch was applied")
		}
	})

	t.Run("connections and relations lacking the required semantics are rejected", func(t *testing.T) {
		// A view has no row identities and no xmin: refusing it is refusing
		// a relation the transactional protocol cannot govern.
		code, body := commit(mainTS, mainToken, "e2e", "op-view-1",
			fmt.Sprintf(`"operations":[{"op":"insert","schema":"public","table":"commits_v","binding":%q,"values":{"id":5,"body":"x"}}]`, binding))
		if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), "not an ordinary table") {
			t.Fatalf("view commit = %d %v, want 400 naming the relation", code, body)
		}
		// Unknown connection.
		code, body = commit(mainTS, mainToken, "ghost", "op-conn-1",
			fmt.Sprintf(`"operations":[{"op":"insert","schema":"public","table":"commits","binding":%q,"values":{"id":6,"body":"x"}}]`, binding))
		if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), "not connected") {
			t.Fatalf("ghost connection = %d %v", code, body)
		}
		// Stale binding on an editable table.
		code, body = commit(mainTS, mainToken, "e2e", "op-binding-1",
			fmt.Sprintf(`"operations":[{"op":"insert","schema":"public","table":"commits","binding":"forged:1","values":{"id":7,"body":"x"}}]`))
		if code != http.StatusConflict || body["state"] != "binding" {
			t.Fatalf("forged binding = %d %v, want 409 binding", code, body)
		}
		var n int
		oracle(`SELECT count(*) FROM commits WHERE id IN (5, 6, 7)`, &n)
		if n != 0 {
			t.Fatal("a refused semantic-violating batch was applied")
		}
	})

	t.Run("S01 F1 regression: a no-key table reports its read-only reason before any binding error", func(t *testing.T) {
		// Forged binding + no-key table: the table-level reason wins over
		// the binding mismatch (the S01 review's entry condition for S02).
		code, body := commit(mainTS, mainToken, "e2e", "op-f1-1",
			`"operations":[{"op":"insert","schema":"public","table":"nokey","binding":"x:1","values":{"a":1}}]`)
		if code != http.StatusBadRequest {
			t.Fatalf("no-key commit with forged binding = %d %v, want 400", code, body)
		}
		if msg, _ := body["error"].(string); !strings.Contains(msg, "no primary key") {
			t.Fatalf("refusal must carry the read-only reason, got: %v", body)
		}
		// The single-op endpoints share the ordering.
		code, body = do(mainTS, mainToken, http.MethodPost, "/api/table/v2/insert",
			`{"connectionId":"e2e","binding":"x:1","schema":"public","table":"nokey","values":{"a":1}}`)
		if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), "no primary key") {
			t.Fatalf("single-op insert on no-key with forged binding = %d %v", code, body)
		}
		var n int
		oracle(`SELECT count(*) FROM nokey`, &n)
		if n != 0 {
			t.Fatal("no-key insert was applied")
		}
	})
}

// currentXmin reads a row's current version through the oracle for staged
// retries in retention tests.
func currentXmin(t *testing.T, oracle func(string, ...any), id int) string {
	t.Helper()
	var v string
	oracle(fmt.Sprintf(`SELECT xmin::text FROM commits WHERE id = %d`, id), &v)
	return v
}
