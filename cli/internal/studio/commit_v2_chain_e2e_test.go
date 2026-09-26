package studio

import (
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

	"github.com/jackc/pgx/v5"
	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestStudioSameRowChainE2E pins the same-row batch contract (R01 review-1
// F1/F2/F3, cases A1-A10): several operations on ONE row staged from one
// read commit together, but a later operation may only run against the
// exact tuple the batch's previous operation on that row produced. Any
// cascade, trigger or rule that rewrites the row, or moves a different row
// under its key, in between conflicts the whole batch.
//
// Everything goes through the REAL route table against a disposable
// Postgres database. Row versions come from real Studio reads, never from
// literals. Oracles are independent SQL on separate sessions; concurrency
// cases are ordered with an advisory-lock gate and pg_locks polling, not
// sleeps. Skipped unless NEUTRON_E2E_DATABASE_URL is set;
// NEUTRON_LIVE_REQUIRED=1 turns a missing URL into a failure. Uses one
// uniquely-named r01c_* database, dropped afterwards.
func TestStudioSameRowChainE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio same-row chain e2e skipped")
	}
	ctx := context.Background()

	dbName := fmt.Sprintf("r01c_%d_%d", os.Getpid(), time.Now().UnixNano())
	dbURL := deriveStudioDatabaseURL(t, base, dbName)
	admin, err := db.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatalf("create database %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if !strings.HasPrefix(dbName, "r01c_") {
			t.Errorf("refusing to drop unexpected database %q", dbName)
			return
		}
		if err := admin.Exec(c, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})

	fixture, err := db.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	t.Cleanup(fixture.Close)
	for _, stmt := range []string{
		`CREATE TABLE parents (id int PRIMARY KEY, code text UNIQUE NOT NULL)`,
		`CREATE TABLE kids (code text REFERENCES parents(code) ON UPDATE CASCADE, n int, x text, PRIMARY KEY (code, n))`,
		`INSERT INTO parents VALUES (1,'A'),(2,'B'),(4,'D')`,
		`INSERT INTO kids VALUES ('A',1,'a-orig'),('B',1,'b-orig')`,
		`CREATE TABLE r (id int PRIMARY KEY, a text, b text)`,
		`INSERT INTO r SELECT g, 'a' || g, 'b' || g FROM generate_series(50, 60) g`,
		`CREATE TABLE s (id int PRIMARY KEY, v text)`,
		`INSERT INTO s VALUES (1, 's-orig')`,
		`CREATE FUNCTION s_touches_r() RETURNS trigger LANGUAGE plpgsql AS $$
		 BEGIN UPDATE r SET b = 'trigger' WHERE id = 50; RETURN NULL; END $$`,
		`CREATE TRIGGER s_touches_r AFTER UPDATE ON s FOR EACH ROW EXECUTE FUNCTION s_touches_r()`,
		// gate blocks every update of its row on an advisory lock the test
		// holds, so a concurrent write can be ordered exactly mid-batch.
		`CREATE TABLE gate (id int PRIMARY KEY, v text)`,
		`INSERT INTO gate VALUES (1, 'gate-orig')`,
		`CREATE FUNCTION gate_wait() RETURNS trigger LANGUAGE plpgsql AS $$
		 BEGIN PERFORM pg_advisory_xact_lock(424242); RETURN NEW; END $$`,
		`CREATE TRIGGER gate_wait BEFORE UPDATE ON gate FOR EACH ROW EXECUTE FUNCTION gate_wait()`,
		// Legacy inheritance: ctid is per physical table, so a cascade can put
		// another row under the chained key at the same ctid in the child
		// table: the edit below leaves the items row at (0,2), and the
		// archive row reaches (0,2) with the test's one foreign write.
		`CREATE TABLE owners (oid int PRIMARY KEY, code int UNIQUE NOT NULL)`,
		`INSERT INTO owners VALUES (1,1),(2,2),(3,3),(4,4)`,
		`CREATE TABLE items (id int PRIMARY KEY REFERENCES owners(code) ON UPDATE CASCADE, x text)`,
		`CREATE TABLE items_archive (FOREIGN KEY (id) REFERENCES owners(code) ON UPDATE CASCADE) INHERITS (items)`,
		`INSERT INTO items VALUES (1,'a-orig')`,
		`UPDATE items SET x = 'a-edited' WHERE id = 1`,
		`INSERT INTO items_archive VALUES (2,'b-orig')`,
		// Declarative partitioning, A1 shape: the cascade moves rows between
		// partitions.
		`CREATE TABLE pparents (id int PRIMARY KEY, code text UNIQUE NOT NULL)`,
		`INSERT INTO pparents VALUES (1,'A'),(2,'B')`,
		`CREATE TABLE pkids (code text REFERENCES pparents(code) ON UPDATE CASCADE, n int, x text, PRIMARY KEY (code, n)) PARTITION BY LIST (code)`,
		`CREATE TABLE pkids_a PARTITION OF pkids FOR VALUES IN ('A')`,
		`CREATE TABLE pkids_b PARTITION OF pkids FOR VALUES IN ('B')`,
		`CREATE TABLE pkids_rest PARTITION OF pkids DEFAULT`,
		`INSERT INTO pkids VALUES ('A',1,'a-orig'),('B',1,'b-orig')`,
	} {
		if err := fixture.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	srv := &Server{
		port: port, sessionToken: fmt.Sprintf("r01c-token-%d", port),
		clients:  map[string]*db.Client{"e2e": fixture},
		outcomes: newOutcomeStore(256, time.Hour, time.Minute),
	}
	mux, err := srv.routes()
	if err != nil {
		t.Fatalf("routes: %v", err)
	}
	ts := httptest.NewUnstartedServer(srv.corsMiddleware(mux))
	ts.Listener = ln
	ts.Start()
	t.Cleanup(ts.Close)
	client := &http.Client{Timeout: 30 * time.Second}

	// cur is the running subtest: helpers report through it (subtests run
	// sequentially), never through the parent test.
	cur := t
	do := func(method, path, body string) (int, map[string]any) {
		cur.Helper()
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rd)
		if err != nil {
			cur.Errorf("request %s %s: %v", method, path, err)
			return 0, nil
		}
		req.Header.Set("Origin", ts.URL)
		req.Header.Set(sessionHeader, srv.sessionToken)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := client.Do(req)
		if err != nil {
			cur.Errorf("do %s %s: %v", method, path, err)
			return 0, nil
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(res.Body)
		var parsed map[string]any
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.UseNumber()
		if err := dec.Decode(&parsed); err != nil {
			cur.Errorf("decode %s %s body %q: %v", method, path, raw, err)
		}
		return res.StatusCode, parsed
	}
	commit := func(opID string, ops ...string) (int, map[string]any) {
		cur.Helper()
		return do(http.MethodPost, "/api/table/v2/commit", fmt.Sprintf(
			`{"connectionId":"e2e","operationId":%q,"operations":[%s]}`, opID, strings.Join(ops, ",")))
	}
	revert := func(opID string) (int, map[string]any) {
		cur.Helper()
		return do(http.MethodPost, "/api/table/v2/revert", fmt.Sprintf(
			`{"connectionId":"e2e","operationId":%q,"revertOperationId":%q}`, opID, opID+"-revert"))
	}

	// read is the SPA's table read: binding plus each row's version, keyed
	// by the key column values joined with "|".
	type readResult struct {
		binding  string
		versions map[string]string
	}
	read := func(table string, keyCols ...string) readResult {
		cur.Helper()
		code, body := do(http.MethodGet, "/api/table?connectionId=e2e&schema=public&table="+table, "")
		if code != http.StatusOK {
			cur.Fatalf("read %s: %d %v", table, code, body)
		}
		cols := body["columns"].([]any)
		idx := make([]int, len(keyCols))
		for k, kc := range keyCols {
			idx[k] = -1
			for i, c := range cols {
				if c == kc {
					idx[k] = i
				}
			}
			if idx[k] < 0 {
				cur.Fatalf("read %s: no column %s in %v", table, kc, cols)
			}
		}
		out := readResult{versions: map[string]string{}}
		out.binding, _ = body["binding"].(string)
		vers := body["versions"].([]any)
		for i, r := range body["rows"].([]any) {
			row := r.([]any)
			parts := make([]string, len(idx))
			for k, j := range idx {
				parts[k] = fmt.Sprint(row[j])
			}
			v, _ := vers[i].(string)
			if v == "" {
				cur.Fatalf("read %s row %v carried no version", table, row)
			}
			out.versions[strings.Join(parts, "|")] = v
		}
		return out
	}

	// Operation builders. key is a JSON array of {column,value} cells.
	upd := func(table, binding, key, version, column, value string) string {
		return fmt.Sprintf(`{"op":"update","schema":"public","table":%q,"binding":%q,"key":%s,"version":%q,"column":%q,"value":%q}`,
			table, binding, key, version, column, value)
	}
	del := func(table, binding, key, version string) string {
		return fmt.Sprintf(`{"op":"delete","schema":"public","table":%q,"binding":%q,"key":%s,"version":%q}`,
			table, binding, key, version)
	}
	ins := func(table, binding, values string) string {
		return fmt.Sprintf(`{"op":"insert","schema":"public","table":%q,"binding":%q,"values":%s}`, table, binding, values)
	}
	idKey := func(id int) string { return fmt.Sprintf(`[{"column":"id","value":%d}]`, id) }
	kidKey := func(code string, n int) string {
		return fmt.Sprintf(`[{"column":"code","value":%q},{"column":"n","value":%d}]`, code, n)
	}

	// snapshot is an independent oracle: every row's text plus xmin, in key
	// order, for the given tables.
	snapshot := func(tables ...string) string {
		cur.Helper()
		var b strings.Builder
		for _, tb := range tables {
			var s string
			if err := fixture.QueryRow(ctx, fmt.Sprintf(
				`SELECT coalesce(string_agg(t::text || '@' || t.xmin::text, ';' ORDER BY t::text), '') FROM %s t`, tb)).Scan(&s); err != nil {
				cur.Fatalf("snapshot %s: %v", tb, err)
			}
			b.WriteString(tb + "=" + s + "\n")
		}
		return b.String()
	}
	scalar := func(query string, args ...any) string {
		cur.Helper()
		var s string
		if err := fixture.QueryRow(ctx, query, args...).Scan(&s); err != nil {
			cur.Fatalf("oracle %q: %v", query, err)
		}
		return s
	}
	wantConflictAt := func(code int, body map[string]any, op int) {
		cur.Helper()
		if code != http.StatusConflict || body["state"] != "conflict" {
			cur.Fatalf("got %d %v, want 409 conflict", code, body)
		}
		if msg, _ := body["error"].(string); !strings.HasPrefix(msg, fmt.Sprintf("operations[%d]", op)) {
			cur.Fatalf("conflict must name operations[%d]: %v", op, body["error"])
		}
	}

	// session opens a dedicated single-connection session (a separate
	// backend, like another user).
	session := func() *pgx.Conn {
		cur.Helper()
		c, err := pgx.Connect(ctx, dbURL)
		if err != nil {
			cur.Fatalf("session: %v", err)
		}
		cur.Cleanup(func() { c.Close(context.Background()) })
		return c
	}
	// waitFor polls until cond holds (condition-based, no fixed sleeps).
	waitFor := func(what string, cond func() bool) {
		cur.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				cur.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	gateWaiters := func() bool {
		var n int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_locks l JOIN pg_database d ON d.oid = l.database
			WHERE d.datname = $1 AND l.locktype = 'advisory' AND NOT l.granted`, dbName).Scan(&n); err != nil {
			cur.Errorf("pg_locks: %v", err)
			return false
		}
		return n > 0
	}
	type httpResult struct {
		code int
		body map[string]any
	}
	commitAsync := func(opID string, ops ...string) chan httpResult {
		ch := make(chan httpResult, 1)
		go func() {
			code, body := commit(opID, ops...)
			ch <- httpResult{code, body}
		}()
		return ch
	}

	run := func(name string, f func(t *testing.T)) {
		t.Run(name, func(st *testing.T) {
			prev := cur
			cur = st
			defer func() { cur = prev }()
			f(st)
		})
	}

	run("A1 a cascade that moves another row under the chained key conflicts (update)", func(t *testing.T) {
		kids, parents := read("kids", "code", "n"), read("parents", "id")
		vA := kids.versions["A|1"]
		// A foreign session edits the row that the cascade will move under
		// the key (A,1). The client never read that write.
		if err := fixture.Exec(ctx, `UPDATE kids SET x = 'foreign' WHERE code = 'B' AND n = 1`); err != nil {
			t.Fatalf("foreign write: %v", err)
		}
		before := snapshot("parents", "kids")
		code, body := commit("a1-update",
			upd("kids", kids.binding, kidKey("A", 1), vA, "x", "first"),
			upd("parents", parents.binding, idKey(1), parents.versions["1"], "code", "Z"),
			upd("parents", parents.binding, idKey(2), parents.versions["2"], "code", "A"),
			upd("kids", kids.binding, kidKey("A", 1), vA, "x", "second"),
		)
		wantConflictAt(code, body, 3)
		wantChainedWording(t, body)
		if after := snapshot("parents", "kids"); after != before {
			t.Fatalf("nothing may apply:\nbefore %s\nafter  %s", before, after)
		}
		if x := scalar(`SELECT x FROM kids WHERE code = 'B' AND n = 1`); x != "foreign" {
			t.Fatalf("foreign write lost: x = %q", x)
		}
	})

	run("A1 a cascade that moves another row under the chained key conflicts (delete)", func(t *testing.T) {
		kids, parents := read("kids", "code", "n"), read("parents", "id")
		vA := kids.versions["A|1"]
		before := snapshot("parents", "kids")
		code, body := commit("a1-delete",
			upd("kids", kids.binding, kidKey("A", 1), vA, "x", "first"),
			upd("parents", parents.binding, idKey(1), parents.versions["1"], "code", "Z"),
			upd("parents", parents.binding, idKey(2), parents.versions["2"], "code", "A"),
			del("kids", kids.binding, kidKey("A", 1), vA),
		)
		wantConflictAt(code, body, 3)
		wantChainedWording(t, body)
		if after := snapshot("parents", "kids"); after != before {
			t.Fatalf("nothing may apply (the wrong row must not be deleted):\nbefore %s\nafter  %s", before, after)
		}
	})

	run("A2 an in-batch trigger rewrite of a chained row conflicts instead of being overwritten", func(t *testing.T) {
		rr, ss := read("r", "id"), read("s", "id")
		v := rr.versions["50"]
		before := snapshot("r", "s")
		code, body := commit("a2",
			upd("r", rr.binding, idKey(50), v, "a", "user-a"),
			upd("s", ss.binding, idKey(1), ss.versions["1"], "v", "fires-trigger"),
			upd("r", rr.binding, idKey(50), v, "b", "user-b"),
		)
		wantConflictAt(code, body, 2)
		wantChainedWording(t, body)
		if after := snapshot("r", "s"); after != before {
			t.Fatalf("nothing may apply:\nbefore %s\nafter  %s", before, after)
		}
	})

	run("A2 control: the same edits without the trigger in between commit", func(t *testing.T) {
		rr := read("r", "id")
		v := rr.versions["50"]
		code, body := commit("a2-control",
			upd("r", rr.binding, idKey(50), v, "a", "user-a"),
			upd("r", rr.binding, idKey(50), v, "b", "user-b"),
			upd("r", rr.binding, idKey(50), v, "a", "user-a2"),
		)
		if code != http.StatusOK {
			t.Fatalf("same-row batch = %d %v, want 200", code, body)
		}
		if got := scalar(`SELECT a || '/' || b FROM r WHERE id = 50`); got != "user-a2/user-b" {
			t.Fatalf("row 50 = %q, want user-a2/user-b", got)
		}
		results := body["operations"].([]any)
		if last := results[2].(map[string]any)["version"]; last != scalar(`SELECT xmin::text FROM r WHERE id = 50`) {
			t.Fatalf("last result version %v is not the row's xmin", last)
		}
	})

	run("A3 a foreign write before the batch's first op on the row conflicts the whole batch", func(t *testing.T) {
		rr, gg := read("r", "id"), read("gate", "id")
		holder, foreign := session(), session()
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock(424242)`); err != nil {
			t.Fatalf("hold gate: %v", err)
		}
		before := snapshot("gate")
		res := commitAsync("a3",
			upd("gate", gg.binding, idKey(1), gg.versions["1"], "v", "slow-first"),
			upd("r", rr.binding, idKey(51), rr.versions["51"], "a", "user-a"),
		)
		waitFor("the batch to wait on the gate", gateWaiters)
		if _, err := foreign.Exec(ctx, `UPDATE r SET a = 'foreign' WHERE id = 51`); err != nil {
			t.Fatalf("foreign write: %v", err)
		}
		foreignRow := snapshot("r")
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock(424242)`); err != nil {
			t.Fatalf("release gate: %v", err)
		}
		got := <-res
		wantConflictAt(got.code, got.body, 1)
		if snapshot("gate") != before || snapshot("r") != foreignRow {
			t.Fatal("the batch must roll back completely, the gated row included, and keep the foreign write")
		}
	})

	run("A4 a foreign write after the first op on the row waits for the batch and is not lost", func(t *testing.T) {
		rr, gg := read("r", "id"), read("gate", "id")
		holder, foreign := session(), session()
		var foreignPID int
		if err := foreign.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&foreignPID); err != nil {
			t.Fatalf("pid: %v", err)
		}
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock(424242)`); err != nil {
			t.Fatalf("hold gate: %v", err)
		}
		res := commitAsync("a4",
			upd("r", rr.binding, idKey(52), rr.versions["52"], "a", "user-a"),
			upd("gate", gg.binding, idKey(1), gg.versions["1"], "v", "slow-second"),
		)
		waitFor("the batch to wait on the gate", gateWaiters)
		foreignDone := make(chan error, 1)
		go func() {
			_, err := foreign.Exec(ctx, `UPDATE r SET b = 'foreign-b' WHERE id = 52`)
			foreignDone <- err
		}()
		waitFor("the foreign write to block on the batch's row lock", func() bool {
			var waiting bool
			if err := admin.QueryRow(ctx, `SELECT coalesce(wait_event_type = 'Lock', false) FROM pg_stat_activity WHERE pid = $1`, foreignPID).Scan(&waiting); err != nil {
				t.Errorf("pg_stat_activity: %v", err)
				return false
			}
			return waiting
		})
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock(424242)`); err != nil {
			t.Fatalf("release gate: %v", err)
		}
		got := <-res
		if got.code != http.StatusOK {
			t.Fatalf("batch = %d %v, want 200", got.code, got.body)
		}
		if err := <-foreignDone; err != nil {
			t.Fatalf("foreign write: %v", err)
		}
		if row := scalar(`SELECT a || '/' || b FROM r WHERE id = 52`); row != "user-a/foreign-b" {
			t.Fatalf("row 52 = %q, want user-a/foreign-b (both writes kept)", row)
		}
	})

	run("A5 two clients from one read, real stale versions and replays", func(t *testing.T) {
		first := read("r", "id")
		v0 := first.versions["53"]
		c1 := upd("r", first.binding, idKey(53), v0, "a", "client-1")
		if code, body := commit("a5-client1", c1); code != http.StatusOK {
			t.Fatalf("client 1 = %d %v", code, body)
		}
		afterC1 := snapshot("r")
		code, body := commit("a5-client2", upd("r", first.binding, idKey(53), v0, "b", "client-2"))
		wantConflictAt(code, body, 0)
		if snapshot("r") != afterC1 {
			t.Fatal("the losing client applied something")
		}

		// A fresh read plus the REAL prior version (from the first read) on
		// one row: the stale one conflicts wherever it sits in the batch.
		v1 := read("r", "id").versions["53"]
		if v1 == v0 {
			t.Fatalf("version did not change after a commit: %q", v1)
		}
		code, body = commit("a5-fresh-stale",
			upd("r", first.binding, idKey(53), v1, "a", "fresh"),
			upd("r", first.binding, idKey(53), v0, "b", "stale"))
		wantConflictAt(code, body, 1)
		code, body = commit("a5-stale-fresh",
			upd("r", first.binding, idKey(53), v0, "b", "stale"),
			upd("r", first.binding, idKey(53), v1, "a", "fresh"))
		wantConflictAt(code, body, 0)
		if snapshot("r") != afterC1 {
			t.Fatal("a batch with a stale version applied something")
		}

		// The committed payload under a new operation ID is a stale write;
		// under the same ID it replays without re-executing.
		code, body = commit("a5-client1-again", c1)
		wantConflictAt(code, body, 0)
		code, body = commit("a5-client1", c1)
		if code != http.StatusOK || body["replayed"] != true {
			t.Fatalf("same-ID replay = %d %v", code, body)
		}
		if snapshot("r") != afterC1 {
			t.Fatal("a replay or a new-ID resend re-executed")
		}
	})

	run("A6 a revert after a partial external change is refused atomically", func(t *testing.T) {
		rr := read("r", "id")
		code, body := commit("a6",
			upd("r", rr.binding, idKey(54), rr.versions["54"], "a", "c54"),
			upd("r", rr.binding, idKey(55), rr.versions["55"], "a", "c55"))
		if code != http.StatusOK || body["reversible"] != true {
			t.Fatalf("commit = %d %v", code, body)
		}
		if err := fixture.Exec(ctx, `UPDATE r SET b = 'extern' WHERE id = 55`); err != nil {
			t.Fatalf("external write: %v", err)
		}
		before := snapshot("r")
		code, body = revert("a6")
		if code != http.StatusConflict {
			t.Fatalf("revert = %d %v, want 409", code, body)
		}
		if snapshot("r") != before {
			t.Fatal("a refused revert applied part of its inverse")
		}
	})

	run("A7 revert order: a cell edited three times and a parent+child insert", func(t *testing.T) {
		rr := read("r", "id")
		v := rr.versions["56"]
		orig := scalar(`SELECT r::text FROM r WHERE id = 56`)
		code, body := commit("a7-cell",
			upd("r", rr.binding, idKey(56), v, "a", "x1"),
			upd("r", rr.binding, idKey(56), v, "a", "x2"),
			upd("r", rr.binding, idKey(56), v, "b", "y1"),
			upd("r", rr.binding, idKey(56), v, "a", "x3"))
		if code != http.StatusOK || body["reversible"] != true {
			t.Fatalf("commit = %d %v", code, body)
		}
		if code, body = revert("a7-cell"); code != http.StatusOK {
			t.Fatalf("revert = %d %v", code, body)
		}
		if got := scalar(`SELECT r::text FROM r WHERE id = 56`); got != orig {
			t.Fatalf("revert left %s, want the exact original %s", got, orig)
		}

		parents, kids := read("parents", "id"), read("kids", "code", "n")
		code, body = commit("a7-insert",
			ins("parents", parents.binding, `{"id":3,"code":"C"}`),
			ins("kids", kids.binding, `{"code":"C","n":1,"x":"k"}`))
		if code != http.StatusOK {
			t.Fatalf("insert batch = %d %v", code, body)
		}
		if code, body = revert("a7-insert"); code != http.StatusOK {
			t.Fatalf("revert (child must be deleted first) = %d %v", code, body)
		}
		if n := scalar(`SELECT (SELECT count(*) FROM parents WHERE id = 3) + (SELECT count(*) FROM kids WHERE code = 'C')`); n != "0" {
			t.Fatalf("revert left %s inserted rows", n)
		}
	})

	run("A8 a chained cascade-key edit whose revert would cascade into a later child is refused", func(t *testing.T) {
		parents := read("parents", "id")
		v := parents.versions["4"]
		code, body := commit("a8",
			upd("parents", parents.binding, idKey(4), v, "code", "E"),
			upd("parents", parents.binding, idKey(4), v, "code", "F"))
		if code != http.StatusOK || body["reversible"] != true {
			t.Fatalf("commit = %d %v (no children yet: reversible)", code, body)
		}
		if err := fixture.Exec(ctx, `INSERT INTO kids VALUES ('F', 1, 'late')`); err != nil {
			t.Fatalf("late child: %v", err)
		}
		before := snapshot("parents", "kids")
		code, body = revert("a8")
		if code != http.StatusConflict || body["state"] != "irreversible" {
			t.Fatalf("revert = %d %v, want 409 irreversible", code, body)
		}
		if snapshot("parents", "kids") != before {
			t.Fatal("the refused revert changed the late child or its parent")
		}
	})

	run("A9 delete, re-insert under the same key, then a chained update conflicts", func(t *testing.T) {
		rr := read("r", "id")
		v := rr.versions["57"]
		before := snapshot("r")
		code, body := commit("a9",
			del("r", rr.binding, idKey(57), v),
			ins("r", rr.binding, `{"id":57,"a":"re","b":"re"}`),
			upd("r", rr.binding, idKey(57), v, "a", "after"))
		wantConflictAt(code, body, 2)
		if snapshot("r") != before {
			t.Fatal("nothing may apply")
		}
	})

	run("A10 the same row addressed by a differently spelled key never chains", func(t *testing.T) {
		rr := read("r", "id")
		v := rr.versions["58"]
		before := snapshot("r")
		code, body := commit("a10",
			upd("r", rr.binding, idKey(58), v, "a", "one"),
			upd("r", rr.binding, `[{"column":"id","value":" 58"}]`, v, "b", "two"))
		if code == http.StatusOK {
			t.Fatalf("a differently spelled key chained: %v", body)
		}
		if snapshot("r") != before {
			t.Fatal("nothing may apply")
		}
	})

	ownerCode := func(binding, version string, oid, code int) string {
		return fmt.Sprintf(`{"op":"update","schema":"public","table":"owners","binding":%q,"key":[{"column":"oid","value":%d}],"version":%q,"column":"code","value":%d}`, binding, oid, version, code)
	}
	for _, last := range []string{"update", "delete"} {
		run("A11 inheritance: a cascade moves a child-table row under the chained key at the same ctid ("+last+")", func(t *testing.T) {
			items, owners := read("items", "id"), read("owners", "oid")
			vA := items.versions["1"]
			if err := fixture.Exec(ctx, `UPDATE items_archive SET x = 'foreign' WHERE id = 2 AND x <> 'foreign'`); err != nil {
				t.Fatalf("foreign write: %v", err)
			}
			if same := scalar(`SELECT ((SELECT ctid FROM ONLY items WHERE id = 1) = (SELECT ctid FROM items_archive WHERE id = 2))::text`); same != "true" {
				t.Fatalf("fixture no longer puts both rows at one ctid")
			}
			op := upd("items", items.binding, idKey(1), vA, "x", "second")
			if last == "delete" {
				op = del("items", items.binding, idKey(1), vA)
			}
			before := snapshot("owners", "items")
			code, body := commit("a11-"+last,
				upd("items", items.binding, idKey(1), vA, "x", "first"),
				ownerCode(owners.binding, owners.versions["1"], 1, 9),
				ownerCode(owners.binding, owners.versions["2"], 2, 1),
				op)
			wantConflictAt(code, body, 3)
			if after := snapshot("owners", "items"); after != before {
				t.Fatalf("nothing may apply:\nbefore %s\nafter  %s", before, after)
			}
		})
	}

	for _, last := range []string{"update", "delete"} {
		run("A12 partitioned: the cascade moves rows between partitions ("+last+")", func(t *testing.T) {
			kids, parents := read("pkids", "code", "n"), read("pparents", "id")
			vA := kids.versions["A|1"]
			if err := fixture.Exec(ctx, `UPDATE pkids SET x = 'foreign' WHERE code = 'B' AND n = 1`); err != nil {
				t.Fatalf("foreign write: %v", err)
			}
			op := upd("pkids", kids.binding, kidKey("A", 1), vA, "x", "second")
			if last == "delete" {
				op = del("pkids", kids.binding, kidKey("A", 1), vA)
			}
			before := snapshot("pparents", "pkids")
			code, body := commit("a12-"+last,
				upd("pkids", kids.binding, kidKey("A", 1), vA, "x", "first"),
				upd("pparents", parents.binding, idKey(1), parents.versions["1"], "code", "Z"),
				upd("pparents", parents.binding, idKey(2), parents.versions["2"], "code", "A"),
				op)
			wantConflictAt(code, body, 3)
			if after := snapshot("pparents", "pkids"); after != before {
				t.Fatalf("nothing may apply:\nbefore %s\nafter  %s", before, after)
			}
		})
	}

	// A foreign-table inheritance child: its rows read xmin 0 and a remote
	// ctid, so no version or tuple check can guard them. The parent is
	// read-only. A loopback postgres_fdw server makes the rows real.
	quoteLiteral := func(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }
	mapping := `CREATE USER MAPPING FOR CURRENT_USER SERVER loop`
	if u, err := url.Parse(base); err == nil && u.User != nil {
		if pw, ok := u.User.Password(); ok {
			mapping += fmt.Sprintf(` OPTIONS (user %s, password %s)`, quoteLiteral(u.User.Username()), quoteLiteral(pw))
		}
	}
	for _, stmt := range []string{
		`CREATE EXTENSION postgres_fdw`,
		fmt.Sprintf(`CREATE SERVER loop FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1', port %s, dbname %s)`,
			quoteLiteral(scalar(`SELECT current_setting('port')`)), quoteLiteral(dbName)),
		mapping,
		`CREATE SCHEMA remote`,
		`CREATE TABLE remote.pt (id int, x text)`,
		`INSERT INTO remote.pt VALUES (1,'orig')`,
		`CREATE TABLE fitems (id int PRIMARY KEY, x text)`,
		`CREATE FOREIGN TABLE fitems_f () INHERITS (fitems) SERVER loop OPTIONS (schema_name 'remote', table_name 'pt')`,
		// The remote relation spans two tables: postgres_fdw updates by the
		// remote ctid, which matches a row in each.
		`CREATE TABLE remote.rt (id int, x text)`,
		`CREATE TABLE remote.rt_c () INHERITS (remote.rt)`,
		`INSERT INTO remote.rt VALUES (1,'a-orig')`,
		`INSERT INTO remote.rt_c VALUES (2,'b-orig')`,
		`CREATE TABLE fitems2 (id int PRIMARY KEY, x text)`,
		`CREATE FOREIGN TABLE fitems2_f () INHERITS (fitems2) SERVER loop OPTIONS (schema_name 'remote', table_name 'rt')`,
	} {
		if err := fixture.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}
	wantForeignReadOnly := func(t *testing.T, table string) {
		t.Helper()
		code, body := do(http.MethodGet, "/api/table?connectionId=e2e&schema=public&table="+table, "")
		reason, _ := body["readOnlyReason"].(string)
		if code != http.StatusOK || body["readOnly"] != true || !strings.Contains(reason, "foreign table") {
			t.Fatalf("%s must read as read-only for its foreign child: %d readOnly=%v reason=%q", table, code, body["readOnly"], reason)
		}
	}

	run("A13 a stale edit of a foreign-child row is refused (the parent is read-only)", func(t *testing.T) {
		fi := read("fitems", "id")
		if err := fixture.Exec(ctx, `UPDATE remote.pt SET x = 'foreign' WHERE id = 1`); err != nil {
			t.Fatalf("foreign write: %v", err)
		}
		code, body := commit("a13-stale", upd("fitems", fi.binding, idKey(1), fi.versions["1"], "x", "stale-client"))
		if code == http.StatusOK {
			t.Errorf("a stale edit of a foreign-child row committed: %v", body)
		}
		if x := scalar(`SELECT x FROM remote.pt WHERE id = 1`); x != "foreign" {
			t.Fatalf("foreign write lost: x = %q", x)
		}
		wantForeignReadOnly(t, "fitems")
	})

	run("A13 a one-row edit of a foreign-child row cannot reach a second remote row", func(t *testing.T) {
		fi := read("fitems2", "id")
		code, body := commit("a13-heaps", upd("fitems2", fi.binding, idKey(1), fi.versions["1"], "x", "solo"))
		if code == http.StatusOK {
			t.Errorf("an edit of a foreign-child row committed: %v", body)
		}
		if got := scalar(`SELECT string_agg(id || ':' || x, ',' ORDER BY id) FROM remote.rt`); got != "1:a-orig,2:b-orig" {
			t.Fatalf("remote rows changed: %s", got)
		}
		wantForeignReadOnly(t, "fitems2")
	})
}

// wantChainedWording checks a chained-operation conflict names the in-batch
// rewrite rather than blaming a foreign write.
func wantChainedWording(t *testing.T, body map[string]any) {
	t.Helper()
	if msg, _ := body["error"].(string); !strings.Contains(msg, "not the one this batch's earlier operation on it left") {
		t.Fatalf("chained conflict wording: %v", body["error"])
	}
}
