package studio

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Unit tests for the S04 SQL editor surface that need no database: request
// validation, guards that refuse BEFORE any connection is touched, the
// cancel registry's dispatch race, and the route table's auth gate. The
// live behavior (real pg_cancel_backend, connection reuse, EXPLAIN
// non-execution with an independent SQL oracle) is in sqlexec_e2e_test.go.

func TestLeadingKeyword(t *testing.T) {
	cases := map[string]string{
		"select 1":                                "SELECT",
		"  \n\tDELETE FROM t":                     "DELETE",
		"-- note\nINSERT INTO t VALUES (1)":       "INSERT",
		"/* a /* nested */ b */ update t set x=1": "UPDATE",
		"(SELECT 1) UNION (SELECT 2)":             "(",
		"with d as (delete from t) select 1":      "WITH",
		"explain analyze delete from t":           "EXPLAIN",
		"":                                        "",
		"-- only a comment":                       "",
		"/* unterminated":                         "",
		"123":                                     "",
	}
	for in, want := range cases {
		if got := leadingKeyword(in); got != want {
			t.Errorf("leadingKeyword(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTrimStatement(t *testing.T) {
	cases := map[string]string{
		"SELECT 1;":         "SELECT 1",
		"  SELECT 1 ; ;\n ": "SELECT 1",
		"SELECT ';'":        "SELECT ';'",
		";;;":               "",
		"SELECT 1; -- note": "SELECT 1; -- note",
	}
	for in, want := range cases {
		if got := trimStatement(in); got != want {
			t.Errorf("trimStatement(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestResolveRequestID(t *testing.T) {
	id, err := resolveRequestID("")
	if err != nil || !requestIDPattern.MatchString(id) || !strings.HasPrefix(id, "srv-") {
		t.Fatalf("minted id %q err %v", id, err)
	}
	id2, _ := resolveRequestID("")
	if id == id2 {
		t.Error("minted request ids must differ")
	}
	if got, err := resolveRequestID("0f8e2c4a-1b2c-4d5e-8f90-123456789abc"); err != nil || got != "0f8e2c4a-1b2c-4d5e-8f90-123456789abc" {
		t.Errorf("uuid id refused: %q %v", got, err)
	}
	for _, bad := range []string{"short", "has space in it", "semi;colon-xxxxxxxx", strings.Repeat("a", 129)} {
		if _, err := resolveRequestID(bad); err == nil {
			t.Errorf("request id %q must be refused", bad)
		}
	}
}

func TestDecodeQueryParams(t *testing.T) {
	var raw []any
	dec := json.NewDecoder(strings.NewReader(`["abc", null, 9007199254740993, true, {"t":"int8","v":"9007199254740993"}, {"t":"bytea","v":"00ff"}, "12.50"]`))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		t.Fatal(err)
	}
	got, err := decodeQueryParams(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// JSON numbers travel as their exact literal text (no float64 detour).
	if got[0] != "abc" || got[1] != nil || got[2] != "9007199254740993" || got[3] != true || got[6] != "12.50" {
		t.Errorf("scalars decoded as %#v", got)
	}
	if v, ok := got[4].(int64); !ok || v != 9007199254740993 {
		t.Errorf("tagged int8 decoded as %#v", got[4])
	}
	if b, ok := got[5].([]byte); !ok || len(b) != 2 || b[0] != 0 || b[1] != 0xff {
		t.Errorf("tagged bytea decoded as %#v", got[5])
	}

	for _, bad := range []string{`[{"a":1}]`, `[[1,2]]`, `[{"t":"int8","v":"x"}]`, `[{"t":"nope","v":"1"}]`} {
		var r []any
		d := json.NewDecoder(strings.NewReader(bad))
		d.UseNumber()
		if err := d.Decode(&r); err != nil {
			t.Fatal(err)
		}
		if _, err := decodeQueryParams(r); err == nil {
			t.Errorf("params %s must be refused", bad)
		} else if !strings.Contains(err.Error(), "$1") {
			t.Errorf("error for %s should name the parameter: %v", bad, err)
		}
	}
}

// postJSON drives the production route table + middleware with the test
// server's own origin and token.
func postEditor(t *testing.T, s *Server, path, body string, auth bool) (int, map[string]any) {
	t.Helper()
	mux, err := s.routes()
	if err != nil {
		t.Fatalf("routes: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Host = "localhost:59999"
	req.Header.Set("Content-Type", "application/json")
	if auth {
		authed(s, req)
	}
	rec := httptest.NewRecorder()
	s.corsMiddleware(mux).ServeHTTP(rec, req)
	var parsed map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &parsed)
	return rec.Code, parsed
}

func TestEditorEndpointsRequireSessionAndOrigin(t *testing.T) {
	s := newAuthTestServer(t)
	for _, path := range []string{"/api/query", "/api/query/cancel", "/api/query/explain"} {
		code, body := postEditor(t, s, path, `{"connectionId":"c","requestId":"abcdefgh","sql":"SELECT 1"}`, false)
		if code != http.StatusForbidden || body["auth"] != "session" {
			t.Errorf("%s without token: %d %v, want 403 auth=session", path, code, body)
		}

		mux, _ := s.routes()
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		req.Host = "localhost:59999"
		req.Header.Set("Origin", "http://localhost:3000")
		req.Header.Set(sessionHeader, s.sessionToken)
		rec := httptest.NewRecorder()
		s.corsMiddleware(mux).ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), `"auth":"origin"`) {
			t.Errorf("%s from another localhost port: %d %s, want 403 auth=origin", path, rec.Code, rec.Body.String())
		}
	}
}

func TestQueryRequestValidation(t *testing.T) {
	s := newAuthTestServer(t)
	cases := []struct {
		body string
		want string
	}{
		{`{"sql":"SELECT 1","connectionId":"c","where":"x"}`, "unknown field"},
		{`{"sql":"SELECT 1","connectionId":"c","requestId":"bad id"}`, "requestId"},
		{`{"sql":"SELECT $1","connectionId":"c","params":[{"a":1}]}`, "$1"},
		{`{"sql":"SELECT 1","connectionId":"nope"}`, "not connected"},
	}
	for _, c := range cases {
		code, body := postEditor(t, s, "/api/query", c.body, true)
		if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), c.want) {
			t.Errorf("query %s: %d %v, want 400 containing %q", c.body, code, body, c.want)
		}
	}
}

func TestExplainGuardsRefuseBeforeTouchingAConnection(t *testing.T) {
	// No client is registered: every case below must be decided before a
	// connection lookup, proving nothing could have been executed.
	s := newAuthTestServer(t)
	cases := []struct {
		body  string
		code  int
		state string
		want  string
	}{
		{`{"connectionId":"c","sql":"SELECT 1","allowWrites":true}`, 400, "", "only to EXPLAIN ANALYZE"},
		{`{"connectionId":"c","sql":"  ;  "}`, 400, "", "sql is required"},
		{`{"connectionId":"c","sql":"explain analyze delete from t"}`, 400, "", "already an EXPLAIN"},
		{`{"connectionId":"c","sql":"DROP TABLE t"}`, 422, "unsupported", "no query plan for a DROP"},
		{`{"connectionId":"c","sql":"DELETE FROM t","analyze":true}`, 422, "write-blocked", "Nothing was executed"},
		{`{"connectionId":"c","sql":"-- hi\ninsert into t values (1);","analyze":true}`, 422, "write-blocked", "allow writes"},
		{`{"connectionId":"c","sql":"MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE","analyze":true}`, 422, "write-blocked", ""},
	}
	for _, c := range cases {
		code, body := postEditor(t, s, "/api/query/explain", c.body, true)
		if code != c.code {
			t.Errorf("explain %s: status %d %v, want %d", c.body, code, body, c.code)
			continue
		}
		if c.state != "" && body["state"] != c.state {
			t.Errorf("explain %s: state %v, want %q", c.body, body["state"], c.state)
		}
		if c.want != "" && !strings.Contains(body["error"].(string), c.want) {
			t.Errorf("explain %s: error %q, want it to contain %q", c.body, body["error"], c.want)
		}
		if c.state == "write-blocked" && body["executed"] != false {
			t.Errorf("explain %s: write-blocked must report executed=false, got %v", c.body, body["executed"])
		}
	}
	// A plain (non-analyze) EXPLAIN of a write is allowed past the guards
	// and only then needs a connection — it never executes the statement.
	code, body := postEditor(t, s, "/api/query/explain", `{"connectionId":"c","sql":"DELETE FROM t"}`, true)
	if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), "not connected") {
		t.Errorf("plain EXPLAIN of a write should reach the connection lookup: %d %v", code, body)
	}
}

func TestCancelUnknownRequestIsNotRunning(t *testing.T) {
	s := newAuthTestServer(t)
	code, body := postEditor(t, s, "/api/query/cancel", `{"connectionId":"c","requestId":"abcdefgh-1"}`, true)
	if code != http.StatusNotFound || body["state"] != "not-running" {
		t.Errorf("cancel unknown: %d %v", code, body)
	}
	code, body = postEditor(t, s, "/api/query/cancel", `{"connectionId":"c","requestId":"x"}`, true)
	if code != http.StatusBadRequest {
		t.Errorf("cancel malformed id: %d %v", code, body)
	}
}

func TestCancelBeforeDispatchNeverSends(t *testing.T) {
	s := newAuthTestServer(t)
	rq := &runningQuery{connID: "c", pid: 4242, hardStop: func() {}}
	reg := s.queryRegistry()
	reg.running["req-before-dispatch"] = rq
	e := &editorExec{s: s, id: "req-before-dispatch", rq: rq}

	// Another connection's ID cannot address the statement.
	if out := s.cancelRunning("other", "req-before-dispatch"); out.State != "not-running" {
		t.Errorf("cross-connection cancel: %+v", out)
	}
	if out := s.cancelRunning("c", "req-before-dispatch"); out.State != "canceled-before-dispatch" {
		t.Fatalf("cancel before dispatch: %+v", out)
	}
	if e.dispatch() {
		t.Fatal("dispatch must refuse after a cancel arrived first")
	}
	// Once settled, a late cancel is a no-op (no signal can reach a backend
	// that may already be serving someone else).
	e.settle()
	if out := s.cancelRunning("c", "req-before-dispatch"); out.State != "not-running" {
		t.Errorf("cancel after settle: %+v", out)
	}
}

func TestCancelWithoutBackendPIDIsUnsupported(t *testing.T) {
	s := newAuthTestServer(t)
	stopped := false
	rq := &runningQuery{connID: "c", pid: 0, dispatched: true, hardStop: func() { stopped = true }}
	s.queryRegistry().running["req-no-pid-1"] = rq
	out := s.cancelRunning("c", "req-no-pid-1")
	if out.State != "unsupported" || out.Err == nil || !strings.Contains(out.Err.Error(), "closed the statement's connection") {
		t.Errorf("pid 0 must report unsupported honestly: %+v", out)
	}
	if !stopped || !rq.hardStopped {
		t.Error("pid 0 cancel must close the statement's connection instead of leaving it running")
	}
}
