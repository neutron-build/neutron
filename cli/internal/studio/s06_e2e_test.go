package studio

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestStudioLargeDataS06E2E drives the S06 surfaces through the REAL route
// table and corsMiddleware against a REAL disposable PostgreSQL database:
// streamed export exactness (int8/numeric digits, NULL vs empty string,
// quoting, bytea, jsonb, temporals) against an independent SQL oracle,
// bounded server memory while streaming a large table, prompt connection
// release on early client exit, the editor's result cap, bounded and
// deterministic table pages, and the import batch contract: per-batch
// atomicity, failing-row attribution, idempotent replay, outcome lookup
// agreeing with the database after an abandoned request, and the honest
// unknown answer after a restart.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL is set; NEUTRON_LIVE_REQUIRED=1
// turns a missing URL into a failure. Uses one s06_* database, dropped
// afterwards (WITH (FORCE)).
func TestStudioLargeDataS06E2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio S06 e2e skipped")
	}
	ctx := context.Background()
	dbName := fmt.Sprintf("s06_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	admin, err := db.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if !strings.HasPrefix(dbName, "s06_") {
			t.Errorf("refusing to drop unexpected database %q", dbName)
			return
		}
		if err := admin.Exec(c, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})
	fixture, err := db.Connect(ctx, deriveStudioDatabaseURL(t, base, dbName))
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	defer fixture.Close()

	for _, stmt := range []string{
		`CREATE TABLE wide (id bigint PRIMARY KEY, amount numeric(50,10), note text, flag boolean,
		   seen_at timestamptz, doc jsonb, raw bytea)`,
		`INSERT INTO wide VALUES
		   (9223372036854775807, 1234567890123456789012345678901234567890.0123456789, NULL, true,
		    '2026-02-03 04:05:06.123456+00', '{"n": 90071992547409931, "s": "x"}', '\x00ff10'),
		   (-9223372036854775808, -0.0000000001, '', false, NULL, 'null', ''),
		   (1, NULL, 'a,"b"' || chr(10) || 'c', NULL, 'infinity', '[1, 2.50]', NULL),
		   (2, 'NaN', ' lead and trail ', true, '1999-12-31 23:59:59.999999+00', '"str"', '\xdeadbeef'),
		   (3, 0, 'NULL', false, '2000-01-01 00:00:00+00', '{}', '\x')`,
		`CREATE TABLE big (id int PRIMARY KEY, payload text NOT NULL)`,
		`INSERT INTO big SELECT g, repeat(md5(g::text), 32) FROM generate_series(1, 60000) g`,
		`CREATE TABLE imp (id int PRIMARY KEY, code text UNIQUE, qty bigint NOT NULL,
		   note text DEFAULT 'dflt', flag boolean)`,
		`CREATE TABLE keyless (a int, b text)`,
		`CREATE TABLE shuffled (id int PRIMARY KEY, v text)`,
		`INSERT INTO shuffled SELECT g, 'v' || g FROM generate_series(1, 50) g`,
		// move the low keys to the physical end of the heap
		`UPDATE shuffled SET v = v || '!' WHERE id <= 10`,
	} {
		if err := fixture.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed: %v\n%s", err, stmt)
		}
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	s := &Server{port: port, sessionToken: fmt.Sprintf("s06-token-%d", port), clients: map[string]*db.Client{"e2e": fixture}}
	mux, err := s.routes()
	if err != nil {
		t.Fatalf("routes: %v", err)
	}
	ts := httptest.NewUnstartedServer(s.corsMiddleware(mux))
	ts.Listener = ln
	ts.Start()
	defer ts.Close()
	client := &http.Client{Timeout: 60 * time.Second}

	do := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Origin", ts.URL)
		req.Header.Set(sessionHeader, s.sessionToken)
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer res.Body.Close()
		var out map[string]any
		dec := json.NewDecoder(res.Body)
		dec.UseNumber()
		_ = dec.Decode(&out)
		return res.StatusCode, out
	}
	ticket := func(body string) string {
		t.Helper()
		code, out := do(http.MethodPost, "/api/table/v2/export", body)
		if code != http.StatusOK {
			t.Fatalf("export request %s: %d %v", body, code, out)
		}
		return out["url"].(string)
	}
	download := func(url string) (int, http.Header, string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, ts.URL+url, nil)
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		res, err := client.Do(req)
		if err != nil {
			t.Fatalf("download: %v", err)
		}
		defer res.Body.Close()
		raw, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("download body: %v", err)
		}
		return res.StatusCode, res.Header, string(raw)
	}
	// oracle: every column's exact text per row, keyed by id text.
	oracle := func(table string, cols []string) map[string][]*string {
		t.Helper()
		exprs := make([]string, len(cols))
		for i, c := range cols {
			if c == "raw" {
				exprs[i] = `'\x' || encode(raw, 'hex')`
			} else {
				exprs[i] = quoteIdent(c) + "::text"
			}
		}
		rows, err := fixture.Query(ctx, fmt.Sprintf("SELECT %s FROM %s", strings.Join(exprs, ", "), quoteIdent(table)))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string][]*string{}
		for rows.Next() {
			vals := make([]*string, len(cols))
			dest := make([]any, len(cols))
			for i := range vals {
				dest[i] = &vals[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			out[*vals[0]] = vals
		}
		return out
	}
	wideCols := []string{"id", "amount", "note", "flag", "seen_at", "doc", "raw"}

	t.Run("export CSV is exact and keeps NULL distinct from the empty string", func(t *testing.T) {
		code, h, body := download(ticket(`{"connectionId":"e2e","schema":"public","table":"wide","format":"csv"}`))
		if code != http.StatusOK {
			t.Fatalf("download %d: %s", code, body)
		}
		if cd := h.Get("Content-Disposition"); cd != `attachment; filename="public.wide.csv"` {
			t.Errorf("Content-Disposition = %q", cd)
		}
		recs := parseQuotedCSV(t, body)
		if strings.Join(derefAll(recs[0]), ",") != strings.Join(wideCols, ",") {
			t.Fatalf("header = %v", derefAll(recs[0]))
		}
		want := oracle("wide", wideCols)
		if len(recs)-1 != len(want) {
			t.Fatalf("exported %d rows, table has %d", len(recs)-1, len(want))
		}
		prevID := ""
		for _, rec := range recs[1:] {
			id := *rec[0]
			exp, ok := want[id]
			if !ok {
				t.Fatalf("exported unknown id %s", id)
			}
			for i := range wideCols {
				if (exp[i] == nil) != (rec[i] == nil) || (exp[i] != nil && *exp[i] != *rec[i]) {
					t.Errorf("id %s column %s: export %v, oracle %v", id, wideCols[i], show(rec[i]), show(exp[i]))
				}
			}
			if prevID != "" && compareIntText(prevID, id) >= 0 {
				t.Errorf("export not in key order: %s before %s", prevID, id)
			}
			prevID = id
		}
	})

	t.Run("export tickets redeem once", func(t *testing.T) {
		url := ticket(`{"connectionId":"e2e","schema":"public","table":"wide","format":"ndjson"}`)
		if code, _, _ := download(url); code != http.StatusOK {
			t.Fatalf("first redeem %d", code)
		}
		if code, _, _ := download(url); code != http.StatusNotFound {
			t.Fatalf("second redeem = %d, want 404", code)
		}
	})

	t.Run("export JSON keeps digits exact and types honest", func(t *testing.T) {
		_, _, body := download(ticket(`{"connectionId":"e2e","schema":"public","table":"wide","format":"json"}`))
		dec := json.NewDecoder(strings.NewReader(body))
		dec.UseNumber()
		var rows []map[string]json.RawMessage
		if err := dec.Decode(&rows); err != nil {
			t.Fatalf("json export does not parse: %v\n%s", err, body)
		}
		want := oracle("wide", wideCols)
		if len(rows) != len(want) {
			t.Fatalf("rows %d, want %d", len(rows), len(want))
		}
		for _, r := range rows {
			id := string(r["id"])
			exp := want[id]
			if exp == nil {
				t.Fatalf("id literal %s is not an exact int8 key", id)
			}
			// numeric: exact literal digits, NaN as a string, NULL as null
			switch {
			case exp[1] == nil:
				if string(r["amount"]) != "null" {
					t.Errorf("id %s amount %s, want null", id, r["amount"])
				}
			case *exp[1] == "NaN":
				if string(r["amount"]) != `"NaN"` {
					t.Errorf("NaN amount = %s", r["amount"])
				}
			default:
				if string(r["amount"]) != *exp[1] {
					t.Errorf("id %s amount %s, oracle %s", id, r["amount"], *exp[1])
				}
			}
			// text: NULL is null, '' is ""
			if exp[2] == nil {
				if string(r["note"]) != "null" {
					t.Errorf("id %s note %s, want null", id, r["note"])
				}
			} else {
				var got string
				if err := json.Unmarshal(r["note"], &got); err != nil || got != *exp[2] {
					t.Errorf("id %s note %s, oracle %q", id, r["note"], *exp[2])
				}
			}
			// jsonb: embedded JSON equal to the stored document text
			if exp[5] != nil && string(r["doc"]) != *exp[5] {
				t.Errorf("id %s doc %s, oracle %s", id, r["doc"], *exp[5])
			}
		}
	})

	t.Run("export honors filters, sorts and match", func(t *testing.T) {
		_, _, body := download(ticket(`{"connectionId":"e2e","schema":"public","table":"wide","format":"ndjson",
			"filters":[{"column":"flag","op":"not-null"}],"sorts":[{"column":"id","dir":"desc"}]}`))
		lines := strings.Split(strings.TrimRight(body, "\n"), "\n")
		var n int64
		if err := fixture.QueryRow(ctx, `SELECT count(*) FROM wide WHERE flag IS NOT NULL`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if int64(len(lines)) != n {
			t.Fatalf("filtered export %d lines, oracle %d", len(lines), n)
		}
		if !strings.HasPrefix(lines[0], `{"id":9223372036854775807,`) {
			t.Errorf("desc sort first line = %s", lines[0])
		}
		code, out := do(http.MethodPost, "/api/table/v2/export", `{"connectionId":"e2e","schema":"public","table":"wide","format":"csv","filters":[{"column":"nope","op":"eq","value":"1"}]}`)
		if code != http.StatusBadRequest {
			t.Errorf("unknown filter column = %d %v", code, out)
		}
	})

	t.Run("large export streams with bounded server memory", func(t *testing.T) {
		url := ticket(`{"connectionId":"e2e","schema":"public","table":"big","format":"csv"}`)
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		var peak atomic.Uint64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ms runtime.MemStats
			for {
				select {
				case <-stop:
					return
				case <-time.After(5 * time.Millisecond):
					runtime.ReadMemStats(&ms)
					if ms.HeapInuse > peak.Load() {
						peak.Store(ms.HeapInuse)
					}
				}
			}
		}()
		start := time.Now()
		req, _ := http.NewRequest(http.MethodGet, ts.URL+url, nil)
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		br := bufio.NewReaderSize(res.Body, 32<<10)
		var bytesRead, lines int64
		for {
			line, err := br.ReadSlice('\n')
			bytesRead += int64(len(line))
			if len(line) > 0 && line[len(line)-1] == '\n' {
				lines++
			}
			if err == io.EOF {
				break
			}
			if err != nil && err != bufio.ErrBufferFull {
				t.Fatalf("read: %v", err)
			}
		}
		res.Body.Close()
		close(stop)
		wg.Wait()
		elapsed := time.Since(start)
		growth := int64(peak.Load()) - int64(before.HeapInuse)
		t.Logf("V16 export: %d bytes, %d lines in %v; heap in use before %d, peak %d (growth %d bytes)",
			bytesRead, lines, elapsed, before.HeapInuse, peak.Load(), growth)
		if lines != 60001 {
			t.Fatalf("exported %d lines, want 60001 (header + 60000 rows)", lines)
		}
		if bytesRead < 60_000_000 {
			t.Fatalf("export unexpectedly small: %d bytes", bytesRead)
		}
		// ~62 MB streamed; the server's working set must not scale with it.
		if growth > 24<<20 {
			t.Errorf("heap grew by %d bytes while streaming %d bytes: export accumulates", growth, bytesRead)
		}
	})

	t.Run("an abandoned download releases its statement and connection promptly", func(t *testing.T) {
		url := ticket(`{"connectionId":"e2e","schema":"public","table":"big","format":"json"}`)
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n\r\n", url, port)
		buf := make([]byte, 64<<10)
		if _, err := io.ReadAtLeast(conn, buf, 32<<10); err != nil {
			t.Fatalf("read head of export: %v", err)
		}
		// the statement is running while the client holds the stream
		active := func() int64 {
			var n int64
			if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
				WHERE datname = $1 AND query LIKE '%"payload"::text%' AND query NOT LIKE '%pg_stat_activity%'
				  AND state <> 'idle'`, dbName).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n
		}
		if active() == 0 {
			t.Log("note: export statement already finished server-side before the probe (fast machine)")
		}
		conn.Close()
		closedAt := time.Now()
		deadline := closedAt.Add(5 * time.Second)
		for active() > 0 {
			if time.Now().After(deadline) {
				t.Fatalf("export statement still running %v after the client left", time.Since(closedAt))
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Logf("V16 early exit: statement gone %v after the client closed", time.Since(closedAt))
		// the pool is usable again (the connection was returned, not leaked)
		c2, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		for i := 0; i < 8; i++ {
			var one int
			if err := fixture.QueryRow(c2, `SELECT 1`).Scan(&one); err != nil {
				t.Fatalf("pool unusable after abandoned export: %v", err)
			}
		}
	})

	t.Run("editor results are capped and say so", func(t *testing.T) {
		code, out := do(http.MethodPost, "/api/query", `{"connectionId":"e2e","sql":"SELECT g FROM generate_series(1, 10005) g"}`)
		if code != http.StatusOK || out["truncated"] != true || fmt.Sprint(out["rowCount"]) != "10000" || fmt.Sprint(out["rowLimit"]) != "10000" {
			t.Fatalf("capped query = %d rowCount=%v truncated=%v", code, out["rowCount"], out["truncated"])
		}
		code, out = do(http.MethodPost, "/api/query", `{"connectionId":"e2e","sql":"SELECT g FROM generate_series(1, 10000) g"}`)
		if code != http.StatusOK || out["truncated"] != nil || fmt.Sprint(out["rowCount"]) != "10000" {
			t.Fatalf("exact-limit query = %d rowCount=%v truncated=%v", code, out["rowCount"], out["truncated"])
		}
		// a statement that fails AFTER the cap still reports its error
		code, out = do(http.MethodPost, "/api/query", `{"connectionId":"e2e","sql":"SELECT CASE WHEN g = 10003 THEN 1/0 ELSE g END FROM generate_series(1, 10005) g"}`)
		if code != http.StatusOK || !strings.Contains(fmt.Sprint(out["error"]), "division by zero") {
			t.Fatalf("late failure = %d %v", code, out["error"])
		}
	})

	t.Run("table pages are bounded and deterministic", func(t *testing.T) {
		code, _ := do(http.MethodGet, "/api/table?connectionId=e2e&schema=public&table=shuffled&limit=1001", "")
		if code != http.StatusBadRequest {
			t.Fatalf("limit 1001 = %d", code)
		}
		seen := map[string]bool{}
		var order []string
		for off := 0; off < 50; off += 20 {
			code, out := do(http.MethodGet, fmt.Sprintf("/api/table?connectionId=e2e&schema=public&table=shuffled&limit=20&offset=%d", off), "")
			if code != http.StatusOK {
				t.Fatalf("page %d = %d %v", off, code, out)
			}
			for _, r := range out["rows"].([]any) {
				id := fmt.Sprint(r.([]any)[0])
				if seen[id] {
					t.Fatalf("row %s repeated across pages", id)
				}
				seen[id] = true
				order = append(order, id)
			}
		}
		if len(seen) != 50 || order[0] != "1" || order[49] != "50" {
			t.Fatalf("pages not in key order / incomplete: %v", order)
		}
	})

	// --- import batches ---

	code, meta := do(http.MethodGet, "/api/table/v2/meta?connectionId=e2e&schema=public&table=imp", "")
	if code != http.StatusOK {
		t.Fatalf("meta imp: %d %v", code, meta)
	}
	binding := meta["binding"].(string)
	batch := func(opID, rows string) (int, map[string]any) {
		return do(http.MethodPost, "/api/table/v2/import/batch", fmt.Sprintf(
			`{"connectionId":"e2e","operationId":%q,"schema":"public","table":"imp","binding":%q,"rows":%s}`, opID, binding, rows))
	}
	checksum := func() string {
		var sum string
		if err := fixture.QueryRow(ctx, `SELECT coalesce(md5(string_agg(t::text || '/' || xmin::text, ',' ORDER BY id)), '') FROM imp t`).Scan(&sum); err != nil {
			t.Fatal(err)
		}
		return sum
	}

	t.Run("a batch commits all rows with NULL, empty and DEFAULT distinct and digits exact", func(t *testing.T) {
		code, out := batch("imp-a", `[
			{"id":1,"code":"a","qty":{"t":"int8","v":"9223372036854775807"},"flag":true},
			{"id":2,"code":"b","qty":{"t":"int8","v":"-9223372036854775808"},"note":null},
			{"id":3,"code":"c","qty":{"t":"int8","v":"9007199254740993"},"note":""}]`)
		if code != http.StatusOK || fmt.Sprint(out["applied"]) != "3" {
			t.Fatalf("batch a = %d %v", code, out)
		}
		rows, err := fixture.Query(ctx, `SELECT id, qty::text, note, note IS NULL, flag IS NULL FROM imp ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		want := []struct {
			qty    string
			note   string
			isNull bool
		}{{"9223372036854775807", "dflt", false}, {"-9223372036854775808", "", true}, {"9007199254740993", "", false}}
		i := 0
		for rows.Next() {
			var id int
			var qty string
			var note *string
			var noteNull, flagNull bool
			if err := rows.Scan(&id, &qty, &note, &noteNull, &flagNull); err != nil {
				t.Fatal(err)
			}
			if qty != want[i].qty || noteNull != want[i].isNull || (note != nil && *note != want[i].note) {
				t.Errorf("row %d: qty %s note %v (null %v), want %+v", id, qty, show(note), noteNull, want[i])
			}
			i++
		}
		if i != 3 {
			t.Fatalf("rows = %d", i)
		}
	})

	t.Run("a failing row rolls back its whole batch and is named", func(t *testing.T) {
		before := checksum()
		code, out := batch("imp-b", `[
			{"id":10,"code":"x","qty":{"t":"int8","v":"1"}},
			{"id":11,"code":"a","qty":{"t":"int8","v":"2"}},
			{"id":12,"code":"z","qty":{"t":"int8","v":"3"}}]`)
		if code != http.StatusConflict || out["state"] != "constraint" || fmt.Sprint(out["failedRow"]) != "1" || fmt.Sprint(out["applied"]) != "0" {
			t.Fatalf("unique violation batch = %d %v", code, out)
		}
		if !strings.HasPrefix(fmt.Sprint(out["error"]), "operations[1]") {
			t.Errorf("error does not name the row: %v", out["error"])
		}
		if checksum() != before {
			t.Fatal("a failed batch changed the table")
		}
		// type input failure at execution (boolean text PostgreSQL refuses)
		code, out = batch("imp-b2", `[{"id":20,"code":"q","qty":{"t":"int8","v":"1"}},{"id":21,"code":"r","qty":{"t":"int8","v":"1"},"flag":"maybe"}]`)
		if code/100 != 4 || fmt.Sprint(out["failedRow"]) != "1" {
			t.Fatalf("bad boolean text = %d %v", code, out)
		}
		// prepare-time refusal (wrong wire shape for bigint): nothing runs
		code, out = batch("imp-b3", `[{"id":30,"code":"s","qty":{"t":"int8","v":"1"}},{"id":31,"code":"t","qty":"5"}]`)
		if code != http.StatusBadRequest || fmt.Sprint(out["failedRow"]) != "1" {
			t.Fatalf("untagged bigint = %d %v", code, out)
		}
		// NOT NULL without a value
		code, out = batch("imp-b4", `[{"id":40,"code":"u"}]`)
		if code/100 != 4 || fmt.Sprint(out["failedRow"]) != "0" {
			t.Fatalf("missing NOT NULL = %d %v", code, out)
		}
		if checksum() != before {
			t.Fatal("refused batches changed the table")
		}
	})

	t.Run("batch IDs replay, conflict and resolve through the outcome lookup", func(t *testing.T) {
		before := checksum()
		code, out := batch("imp-a", `[
			{"id":1,"code":"a","qty":{"t":"int8","v":"9223372036854775807"},"flag":true},
			{"id":2,"code":"b","qty":{"t":"int8","v":"-9223372036854775808"},"note":null},
			{"id":3,"code":"c","qty":{"t":"int8","v":"9007199254740993"},"note":""}]`)
		if code != http.StatusOK || out["replayed"] != true || checksum() != before {
			t.Fatalf("replay = %d %v", code, out)
		}
		code, out = batch("imp-a", `[{"id":99,"code":"new","qty":{"t":"int8","v":"1"}}]`)
		if code != http.StatusConflict || out["state"] != "operation_conflict" || checksum() != before {
			t.Fatalf("same id, different payload = %d %v", code, out)
		}
		code, out = do(http.MethodPost, "/api/table/v2/import/outcome", `{"connectionId":"e2e","operationId":"imp-a"}`)
		if code != http.StatusOK || out["state"] != outcomeCommitted {
			t.Fatalf("outcome imp-a = %v", out)
		}
		code, out = do(http.MethodPost, "/api/table/v2/import/outcome", `{"connectionId":"e2e","operationId":"imp-b"}`)
		if code != http.StatusOK || out["state"] != outcomeFailed {
			t.Fatalf("outcome imp-b = %v", out)
		}
	})

	t.Run("an abandoned batch request resolves to the state the database is in", func(t *testing.T) {
		rows := make([]string, 100)
		for i := range rows {
			rows[i] = fmt.Sprintf(`{"id":%d,"code":"d%d","qty":{"t":"int8","v":"%d"}}`, 1000+i, i, i)
		}
		payload := fmt.Sprintf(`{"connectionId":"e2e","operationId":"imp-drop","schema":"public","table":"imp","binding":%q,"rows":[%s]}`,
			binding, strings.Join(rows, ","))
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(conn, "POST /api/table/v2/import/batch HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nOrigin: %s\r\n%s: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
			port, ts.URL, sessionHeader, s.sessionToken, len(payload), payload)
		conn.Close() // the response is dropped
		var state string
		deadline := time.Now().Add(10 * time.Second)
		for {
			_, out := do(http.MethodPost, "/api/table/v2/import/outcome", `{"connectionId":"e2e","operationId":"imp-drop"}`)
			state = fmt.Sprint(out["state"])
			if state == outcomeCommitted || state == outcomeFailed {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("outcome never settled (last %q)", state)
			}
			time.Sleep(20 * time.Millisecond)
		}
		var n int64
		if err := fixture.QueryRow(ctx, `SELECT count(*) FROM imp WHERE id BETWEEN 1000 AND 1099`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		switch state {
		case outcomeCommitted:
			if n != 100 {
				t.Fatalf("outcome committed but %d of 100 rows present", n)
			}
		case outcomeFailed:
			if n != 0 {
				t.Fatalf("outcome failed but %d rows present (partial batch)", n)
			}
		}
		t.Logf("abandoned batch settled as %s with %d rows present", state, n)
	})

	t.Run("a restarted server answers unknown, never a guess", func(t *testing.T) {
		fresh := &Server{port: port, sessionToken: s.sessionToken, clients: map[string]*db.Client{"e2e": fixture}}
		rec, state := fresh.importOutcomeRecords().lookup(outcomeKey("e2e", "imp-a"))
		if rec != nil || (state != "absent" && state != outcomeUnknown) {
			t.Fatalf("fresh server outcome = %v %q", rec, state)
		}
	})

	t.Run("keyless tables refuse imports (read-only gate)", func(t *testing.T) {
		code, meta := do(http.MethodGet, "/api/table/v2/meta?connectionId=e2e&schema=public&table=keyless", "")
		if code != http.StatusOK {
			t.Fatalf("meta keyless %d", code)
		}
		b, _ := meta["binding"].(string)
		code, out := do(http.MethodPost, "/api/table/v2/import/batch", fmt.Sprintf(
			`{"connectionId":"e2e","operationId":"imp-k","schema":"public","table":"keyless","binding":%q,"rows":[{"a":1}]}`, b))
		if code != http.StatusBadRequest || !strings.Contains(fmt.Sprint(out["error"]), "no primary key") {
			t.Fatalf("keyless import = %d %v", code, out)
		}
	})
}

// parseQuotedCSV is the test's own reader: RFC 4180 with the quoted flag
// kept, so an unquoted empty field (SQL NULL) is nil and "" is "".
func parseQuotedCSV(t *testing.T, s string) [][]*string {
	t.Helper()
	var recs [][]*string
	var rec []*string
	i := 0
	for i < len(s) {
		if s[i] == '"' {
			var b strings.Builder
			i++
			for {
				if i >= len(s) {
					t.Fatalf("unterminated quote")
				}
				if s[i] == '"' {
					if i+1 < len(s) && s[i+1] == '"' {
						b.WriteByte('"')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(s[i])
				i++
			}
			v := b.String()
			rec = append(rec, &v)
		} else {
			j := i
			for j < len(s) && s[j] != ',' && s[j] != '\n' {
				j++
			}
			if j == i {
				rec = append(rec, nil)
			} else {
				v := s[i:j]
				rec = append(rec, &v)
			}
			i = j
		}
		if i < len(s) && s[i] == ',' {
			i++
			continue
		}
		recs = append(recs, rec)
		rec = nil
		i++ // newline
	}
	return recs
}

func derefAll(vals []*string) []string {
	out := make([]string, len(vals))
	for i, v := range vals {
		if v != nil {
			out[i] = *v
		}
	}
	return out
}

func show(v *string) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%q", *v)
}

// compareIntText compares two decimal integer strings numerically.
func compareIntText(a, b string) int {
	na, nb := strings.HasPrefix(a, "-"), strings.HasPrefix(b, "-")
	if na != nb {
		if na {
			return -1
		}
		return 1
	}
	sign := 1
	if na {
		a, b, sign = a[1:], b[1:], -1
	}
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -sign
		}
		return sign
	}
	return sign * strings.Compare(a, b)
}
