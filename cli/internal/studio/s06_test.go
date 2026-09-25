package studio

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// Offline (no database) tests for S06: export encoders, ticket lifecycle,
// query building, route guards and import request validation. The live
// counterparts (exact round-trips, streaming, atomic batches, recovery)
// are in s06_e2e_test.go.

func csvOf(vals ...*string) string {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	for i, v := range vals {
		if i > 0 {
			w.WriteByte(',')
		}
		writeCSVField(w, v)
	}
	w.Flush()
	return buf.String()
}

func sp(s string) *string { return &s }

func TestExportCSVKeepsNullAndEmptyDistinct(t *testing.T) {
	cases := []struct {
		in   *string
		want string
	}{
		{nil, ``},
		{sp(""), `""`},
		{sp("plain"), `plain`},
		{sp("a,b"), `"a,b"`},
		{sp(`he said "hi"`), `"he said ""hi"""`},
		{sp("line\nbreak"), "\"line\nbreak\""},
		{sp("cr\rhere"), "\"cr\rhere\""},
		{sp(" lead"), `" lead"`},
		{sp("trail\t"), "\"trail\t\""},
		{sp("mid dle"), `mid dle`},
		{sp(`NULL`), `NULL`},
		{sp(`9223372036854775807`), `9223372036854775807`},
	}
	for _, c := range cases {
		if got := csvOf(c.in); got != c.want {
			t.Errorf("writeCSVField(%v) = %q, want %q", c.in, got, c.want)
		}
	}
	if got := csvOf(nil, sp(""), nil); got != `,"",` {
		t.Errorf("NULL,'',NULL row = %q", got)
	}
}

func TestExportJSONCellTypes(t *testing.T) {
	col := func(oid uint32) tableColumnMeta { return tableColumnMeta{Name: "c", TypeOID: oid} }
	cases := []struct {
		col  tableColumnMeta
		in   *string
		want string
	}{
		{col(oidBool), sp("t"), `true`},
		{col(oidBool), sp("f"), `false`},
		{col(oidInt8), sp("9223372036854775807"), `9223372036854775807`},
		{col(oidInt8), sp("-9223372036854775808"), `-9223372036854775808`},
		{col(oidNumeric), sp("1234567890123456789012345678901234567890.0001"), `1234567890123456789012345678901234567890.0001`},
		{col(oidNumeric), sp("NaN"), `"NaN"`},
		{col(oidNumeric), sp("Infinity"), `"Infinity"`},
		{col(oidFloat8), sp("1e+300"), `1e+300`},
		{col(oidFloat8), sp("-Infinity"), `"-Infinity"`},
		{col(oidInt4), sp("42"), `42`},
		{col(oidJSONB), sp(`{"a": [1, 2.50, 90071992547409931]}`), `{"a": [1, 2.50, 90071992547409931]}`},
		{col(oidJSON), sp(`not json`), `"not json"`},
		{col(25), sp("x\"y"), `"x\"y"`},
		{col(25), sp(""), `""`},
		{col(25), nil, `null`},
		{col(oidTimestamptz), sp("2026-01-02 03:04:05.123456+00"), `"2026-01-02 03:04:05.123456+00"`},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		w := bufio.NewWriter(&buf)
		writeJSONCell(w, c.col, c.in)
		w.Flush()
		if buf.String() != c.want {
			t.Errorf("oid %d %v -> %s, want %s", c.col.TypeOID, c.in, buf.String(), c.want)
		}
	}
}

func TestRowEncoderFormats(t *testing.T) {
	cols := []tableColumnMeta{{Name: "id", TypeOID: oidInt8}, {Name: "note", TypeOID: 25}}
	rows := [][]*string{{sp("1"), nil}, {sp("2"), sp("")}}
	render := func(format string, rows [][]*string) string {
		var buf bytes.Buffer
		enc := newRowEncoder(&buf, format, cols)
		if err := enc.begin(); err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			if err := enc.row(r); err != nil {
				t.Fatal(err)
			}
		}
		if err := enc.end(); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	if got, want := render("csv", rows), "id,note\n1,\n2,\"\"\n"; got != want {
		t.Errorf("csv = %q, want %q", got, want)
	}
	if got, want := render("json", rows), "[\n{\"id\":1,\"note\":null},\n{\"id\":2,\"note\":\"\"}\n]\n"; got != want {
		t.Errorf("json = %q, want %q", got, want)
	}
	if got, want := render("json", nil), "[]\n"; got != want {
		t.Errorf("empty json = %q, want %q", got, want)
	}
	if got, want := render("ndjson", rows), "{\"id\":1,\"note\":null}\n{\"id\":2,\"note\":\"\"}\n"; got != want {
		t.Errorf("ndjson = %q, want %q", got, want)
	}
	// Every JSON document parses (with exact numbers).
	for _, f := range []string{"json"} {
		var v any
		dec := json.NewDecoder(strings.NewReader(render(f, rows)))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			t.Errorf("%s export is not valid JSON: %v", f, err)
		}
	}
}

func TestExportTicketsAreSingleUseBoundedAndExpire(t *testing.T) {
	st := newExportTicketStore(time.Minute, 2)
	now := time.Now()
	st.put("a", &exportJob{created: now})
	if st.take("a", now) == nil {
		t.Fatal("fresh ticket must redeem")
	}
	if st.take("a", now) != nil {
		t.Fatal("a ticket must redeem exactly once")
	}
	st.put("b", &exportJob{created: now})
	if st.take("b", now.Add(2*time.Minute)) != nil {
		t.Fatal("an expired ticket must not redeem")
	}
	st.put("c", &exportJob{created: now})
	st.put("d", &exportJob{created: now})
	st.put("e", &exportJob{created: now})
	if st.take("c", now) != nil {
		t.Fatal("the oldest ticket beyond capacity must be dropped")
	}
	if st.take("d", now) == nil || st.take("e", now) == nil {
		t.Fatal("tickets within capacity must redeem")
	}
	if st.take("", now) != nil {
		t.Fatal("an empty ticket never redeems")
	}
}

func TestExportFilenameIsSafe(t *testing.T) {
	cases := map[[2]string]string{
		{"public", "users"}:           "public.users.csv",
		{"odd schema", `we"ird/../t`}: "odd_schema.we_ird_.._t.csv",
		{"", ""}:                      "export.csv",
		{"ünï", "cödé"}:               "n_.c_d.csv",
	}
	for in, want := range cases {
		if got := exportFilename(in[0], in[1], "csv"); got != want {
			t.Errorf("exportFilename(%q,%q) = %q, want %q", in[0], in[1], got, want)
		}
	}
}

func exportTestMeta() *tableMeta {
	cols := []tableColumnMeta{
		{Name: "tenant", TypeOID: 23, IsPK: true, KeyPos: 1},
		{Name: "id", TypeOID: oidInt8, IsPK: true, KeyPos: 2},
		{Name: "blob", TypeOID: oidBytea},
		{Name: "note", TypeOID: 25},
	}
	m := &tableMeta{Exists: true, Columns: map[string]tableColumnMeta{}, PKCols: []string{"tenant", "id"}}
	for _, c := range cols {
		m.Columns[c.Name] = c
		m.Order = append(m.Order, c)
	}
	return m
}

func TestBuildExportQueryIsExactAndDeterministic(t *testing.T) {
	m := exportTestMeta()
	q, args, err := buildExportQuery(m, "public", "t", nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT "tenant"::text, "id"::text, '\x' || encode("blob", 'hex'), "note"::text FROM "public"."t" ORDER BY "t"."tenant" ASC, "t"."id" ASC`
	if q != want || len(args) != 0 {
		t.Errorf("query = %s (args %v)\nwant    %s", q, args, want)
	}

	q, args, err = buildExportQuery(m, "public", "t",
		json.RawMessage(`[{"column":"note","op":"eq","value":"x"}]`),
		json.RawMessage(`[{"column":"id","dir":"desc"}]`),
		json.RawMessage(`[{"column":"tenant","value":7}]`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, `WHERE "note" = $1 AND "tenant" = $2`) {
		t.Errorf("conditions not bound in order: %s", q)
	}
	// user sort first, then the remaining key column as tiebreaker; the
	// already-sorted key column is not repeated
	if !strings.HasSuffix(q, `ORDER BY "t"."id" DESC, "t"."tenant" ASC`) {
		t.Errorf("order = %s", q)
	}
	if len(args) != 2 || args[0] != "x" || args[1] != "7" {
		t.Errorf("args = %#v", args)
	}

	for _, bad := range []struct{ f, s, m string }{
		{`[{"column":"nope","op":"eq","value":"x"}]`, ``, ``},
		{``, `[{"column":"nope","dir":"asc"}]`, ``},
		{``, `[{"column":"id","dir":"sideways"}]`, ``},
		{``, ``, `[{"column":"nope","value":1}]`},
		{`[{"column":"note","op":"drop","value":"x"}]`, ``, ``},
	} {
		if _, _, err := buildExportQuery(m, "public", "t", json.RawMessage(bad.f), json.RawMessage(bad.s), json.RawMessage(bad.m)); err == nil {
			t.Errorf("expected refusal for %+v", bad)
		}
	}

	// No primary key: no invented order.
	nok := &tableMeta{Exists: true, Columns: map[string]tableColumnMeta{"a": {Name: "a", TypeOID: 25}}, Order: []tableColumnMeta{{Name: "a", TypeOID: 25}}}
	q, _, _ = buildExportQuery(nok, "public", "t", nil, nil, json.RawMessage(`null`))
	if strings.Contains(q, "ORDER BY") {
		t.Errorf("keyless table must not get an order: %s", q)
	}
}

func TestFailedOpIndex(t *testing.T) {
	if got := failedOpIndex(fmtOpError(7, errors.New("boom"))); got != 7 {
		t.Errorf("wrapped index = %d", got)
	}
	if got := failedOpIndex(fmtOpError(3, mutationDomainError{msg: "x"})); got != 3 {
		t.Errorf("domain index = %d", got)
	}
	if got := failedOpIndex(errors.New("no index")); got != -1 {
		t.Errorf("unindexed = %d", got)
	}
}

// s06Server builds the production route table behind corsMiddleware with
// no database connections.
func s06Server(t *testing.T, clients map[string]*db.Client) (*httptest.Server, string) {
	t.Helper()
	s := &Server{port: 4983, sessionToken: "s06-token", clients: clients}
	mux, err := s.routes()
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(s.corsMiddleware(mux)), s.sessionToken
}

func s06Do(t *testing.T, ts *httptest.Server, method, path, body string, headers map[string]string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	// corsMiddleware checks the Host against the configured loopback port.
	req.Host = "localhost:4983"
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

func TestS06RoutesAreGuarded(t *testing.T) {
	ts, token := s06Server(t, map[string]*db.Client{})
	defer ts.Close()
	auth := map[string]string{sessionHeader: token, "Content-Type": "application/json"}

	// Mutation-class endpoints refuse a missing token and a foreign origin.
	for _, path := range []string{"/api/table/v2/export", "/api/table/v2/import/batch", "/api/table/v2/import/outcome"} {
		if code, body := s06Do(t, ts, http.MethodPost, path, `{}`, nil); code != http.StatusForbidden || body["auth"] != "session" {
			t.Errorf("%s without token = %d %v", path, code, body)
		}
		if code, body := s06Do(t, ts, http.MethodPost, path, `{}`, map[string]string{sessionHeader: token, "Origin": "http://localhost:9999"}); code != http.StatusForbidden || body["auth"] != "origin" {
			t.Errorf("%s from a foreign origin = %d %v", path, code, body)
		}
	}

	// The download is a GET but redeems only a real ticket and refuses a
	// browser-marked cross-site request before looking at it.
	if code, _ := s06Do(t, ts, http.MethodGet, "/api/table/v2/export/download?ticket=nope", "", nil); code != http.StatusNotFound {
		t.Errorf("unknown ticket = %d", code)
	}
	if code, body := s06Do(t, ts, http.MethodGet, "/api/table/v2/export/download?ticket=nope", "", map[string]string{"Sec-Fetch-Site": "cross-site"}); code != http.StatusForbidden || body["auth"] != "origin" {
		t.Errorf("cross-site download = %d %v", code, body)
	}

	// Export validation happens before any database work.
	for _, c := range []struct {
		body string
		want int
	}{
		{`{"connectionId":"x","schema":"public","table":"t","format":"xlsx"}`, http.StatusBadRequest},
		{`{"connectionId":"x","schema":"public","table":"t","format":"csv"}`, http.StatusBadRequest}, // not connected
		{`{"connectionId":"","schema":"public","table":"t","format":"csv"}`, http.StatusBadRequest},
		{`{"connectionId":"x","schema":"public","table":"t","format":"csv","sql":"DROP TABLE t"}`, http.StatusBadRequest}, // smuggled field
	} {
		if code, _ := s06Do(t, ts, http.MethodPost, "/api/table/v2/export", c.body, auth); code != c.want {
			t.Errorf("export %s = %d, want %d", c.body, code, c.want)
		}
	}

	// Table reads are bounded per page.
	if code, body := s06Do(t, ts, http.MethodGet, "/api/table?connectionId=x&schema=public&table=t&limit=1001", "", nil); code != http.StatusBadRequest || !strings.Contains(fmt.Sprint(body["error"]), "at most 1000") {
		t.Errorf("limit 1001 = %d %v", code, body)
	}
}

func TestImportBatchValidation(t *testing.T) {
	ts, token := s06Server(t, map[string]*db.Client{})
	defer ts.Close()
	auth := map[string]string{sessionHeader: token, "Content-Type": "application/json"}
	rows := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = fmt.Sprintf(`{"id":%d}`, i)
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	cases := []struct {
		name, body, wantErr string
	}{
		{"no operation id", `{"connectionId":"x","schema":"public","table":"t","binding":"b","rows":[{}]}`, "operationId is required"},
		{"no binding", `{"connectionId":"x","operationId":"i1","schema":"public","table":"t","rows":[{}]}`, "binding are required"},
		{"no rows", `{"connectionId":"x","operationId":"i1","schema":"public","table":"t","binding":"b","rows":[]}`, "at least one operation"},
		{"null row", `{"connectionId":"x","operationId":"i1","schema":"public","table":"t","binding":"b","rows":[null]}`, "rows[0]"},
		{"too many rows", `{"connectionId":"x","operationId":"i1","schema":"public","table":"t","binding":"b","rows":` + rows(101) + `}`, "limited to 100"},
		{"smuggled update", `{"connectionId":"x","operationId":"i1","schema":"public","table":"t","binding":"b","rows":[{}],"key":[]}`, "unknown field"},
	}
	for _, c := range cases {
		code, body := s06Do(t, ts, http.MethodPost, "/api/table/v2/import/batch", c.body, auth)
		if code != http.StatusBadRequest || !strings.Contains(fmt.Sprint(body["error"]), c.wantErr) {
			t.Errorf("%s: %d %v (want 400 containing %q)", c.name, code, body, c.wantErr)
		}
	}
	// Not connected: the batch is recorded as failed (nothing applied), and
	// the outcome lookup reports it.
	code, body := s06Do(t, ts, http.MethodPost, "/api/table/v2/import/batch",
		`{"connectionId":"x","operationId":"i-nc","schema":"public","table":"t","binding":"b","rows":[{}]}`, auth)
	if code != http.StatusBadRequest || body["applied"] != float64(0) {
		t.Errorf("not connected = %d %v", code, body)
	}
	code, body = s06Do(t, ts, http.MethodPost, "/api/table/v2/import/outcome", `{"connectionId":"x","operationId":"i-nc"}`, auth)
	if code != http.StatusOK || body["state"] != outcomeFailed {
		t.Errorf("outcome = %d %v", code, body)
	}
	code, body = s06Do(t, ts, http.MethodPost, "/api/table/v2/import/outcome", `{"connectionId":"x","operationId":"never"}`, auth)
	if code != http.StatusOK || body["state"] != outcomeUnknown {
		t.Errorf("never-seen outcome = %d %v", code, body)
	}
}

func TestImportOutcomesDoNotEvictCommitOutcomes(t *testing.T) {
	s := &Server{}
	if s.importOutcomeRecords() == s.outcomeRecords() {
		t.Fatal("import batches must use their own outcome store")
	}
}
