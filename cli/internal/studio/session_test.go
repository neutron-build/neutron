package studio

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newAuthTestServer builds a Server with a known session token for handler
// tests that pass the auth gate.
func newAuthTestServer(t *testing.T) *Server {
	t.Helper()
	return &Server{port: 59999, sessionToken: "test-token-0123456789abcdef"}
}

// authed marks a request as coming from the test server's own origin with
// its session token.
func authed(s *Server, req *http.Request) *http.Request {
	req.Header.Set("Origin", "http://localhost:59999")
	req.Header.Set(sessionHeader, s.sessionToken)
	return req
}

func TestSessionTokenPerLaunch(t *testing.T) {
	s1 := &Server{port: 1, sessionToken: "aaaaaaaaaaaaaaaa"}
	s2 := &Server{port: 1, sessionToken: "bbbbbbbbbbbbbbbb"}
	if s1.tokenValid(s2.sessionToken) {
		t.Error("token from another launch must not validate")
	}
	if !s1.tokenValid(s1.sessionToken) {
		t.Error("own token must validate")
	}
	if s1.tokenValid("") || (&Server{}).tokenValid("x") {
		t.Error("empty token states must never validate")
	}
}

func TestNewServerGeneratesToken(t *testing.T) {
	// NewServer touches the connection store on disk; use a temp HOME so
	// the test never reads the developer's real store.
	t.Setenv("HOME", t.TempDir())
	s, err := NewServer(0)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer s.store.Remove("")
	if len(s.sessionToken) != 64 {
		t.Errorf("session token = %d hex chars, want 64 (256-bit)", len(s.sessionToken))
	}
	s2, err := NewServer(0)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if s.sessionToken == s2.sessionToken {
		t.Error("two launches must not share a session token")
	}
}

func TestAllowedOriginsExactMatch(t *testing.T) {
	s := &Server{port: 59999}

	allowed := []string{
		"http://localhost:59999",
		"http://127.0.0.1:59999",
		"http://[::1]:59999",
	}
	for _, o := range allowed {
		if !s.originAllowed(o) {
			t.Errorf("launch origin %q should be allowed", o)
		}
	}
	// Wrong port, wrong scheme, superdomain, trailing path: all rejected.
	for _, o := range []string{
		"http://localhost:59998",
		"http://localhost:599990",
		"https://localhost:59999",
		"http://sub.localhost:59999",
		"http://localhost:59999/",
		"http://evil.example",
		"null",
	} {
		if s.originAllowed(o) {
			t.Errorf("origin %q must not be allowed", o)
		}
	}
}

func TestDevOriginEnv(t *testing.T) {
	s := &Server{port: 59999}
	t.Setenv(devOriginEnv, "http://localhost:5173")
	if !s.originAllowed("http://localhost:5173") {
		t.Error("configured dev origin must be allowed exactly")
	}
	if s.originAllowed("http://localhost:5174") {
		t.Error("dev origin allowlist must not leak to other ports")
	}
}

// TestMutationAuthMatrix is the V06 origin/token negative matrix: wrong
// origin, missing token, wrong token, cross-session (replayed) token, and
// the two positive shapes (browser origin + token; no Origin + token).
func TestMutationAuthMatrix(t *testing.T) {
	s := newAuthTestServer(t)
	older := &Server{port: 59999, sessionToken: "replayed-earlier-launch"}

	post := func(origin, token string) int {
		req := httptest.NewRequest(http.MethodPost, "/api/table/update", strings.NewReader(`{}`))
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if token != "" {
			req.Header.Set(sessionHeader, token)
		}
		rec := httptest.NewRecorder()
		s.handleTableRowUpdate(rec, req)
		return rec.Code
	}

	if code := post("http://localhost:59999", s.sessionToken); code != http.StatusBadRequest {
		t.Errorf("valid origin+token should reach body validation (400), got %d", code)
	}
	if code := post("", s.sessionToken); code != http.StatusBadRequest {
		t.Errorf("no-Origin request (curl-like) with token should reach validation (400), got %d", code)
	}
	if code := post("http://localhost:59998", s.sessionToken); code != http.StatusForbidden {
		t.Errorf("wrong origin must 403, got %d", code)
	}
	if code := post("http://evil.example", s.sessionToken); code != http.StatusForbidden {
		t.Errorf("foreign origin must 403, got %d", code)
	}
	if code := post("http://localhost:59999", ""); code != http.StatusForbidden {
		t.Errorf("missing token must 403, got %d", code)
	}
	if code := post("http://localhost:59999", "wrong-token"); code != http.StatusForbidden {
		t.Errorf("wrong token must 403, got %d", code)
	}
	if code := post("http://localhost:59999", older.sessionToken); code != http.StatusForbidden {
		t.Errorf("replayed cross-session token must 403, got %d", code)
	}
	if code := post("", older.sessionToken); code != http.StatusForbidden {
		t.Errorf("replayed token without origin must 403, got %d", code)
	}
}

func TestSessionEndpoint(t *testing.T) {
	s := newAuthTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/session", nil)
	req.Header.Set("Origin", "http://localhost:59999")
	rec := httptest.NewRecorder()
	s.handleSession(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Token != s.sessionToken {
		t.Errorf("token = %q, want the launch token", body.Token)
	}

	// Another origin may send the request but gets nothing useful.
	req = httptest.NewRequest(http.MethodGet, "/api/session", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec = httptest.NewRecorder()
	s.handleSession(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("cross-origin session read must 403, got %d", rec.Code)
	}
}

// TestMiddlewareBoundary drives the real middleware: DNS-rebinding Host
// refusal, exact-origin CORS (no other localhost port), and the central
// session gate on every state-changing /api/ request.
func TestMiddlewareBoundary(t *testing.T) {
	s := newAuthTestServer(t)
	reached := false
	h := s.corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	do := func(method, path, host, origin, token string, extra map[string]string) *httptest.ResponseRecorder {
		reached = false
		req := httptest.NewRequest(method, path, strings.NewReader(`{}`))
		req.Host = host
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if token != "" {
			req.Header.Set(sessionHeader, token)
		}
		for k, v := range extra {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	const self = "http://localhost:59999"
	good := "localhost:59999"

	if rec := do("GET", "/api/table", "attacker.example:59999", "", "", nil); rec.Code != http.StatusForbidden || reached {
		t.Errorf("rebinding Host = %d reached=%v, want 403", rec.Code, reached)
	}
	if rec := do("GET", "/api/table", good, "", "", nil); rec.Code != http.StatusOK || !reached {
		t.Errorf("same-origin read = %d, want 200", rec.Code)
	}
	if rec := do("GET", "/api/table", "127.0.0.1:59999", "", "", nil); rec.Code != http.StatusOK {
		t.Errorf("127.0.0.1 Host read = %d, want 200", rec.Code)
	}
	// Another localhost app can no longer read Studio data cross-origin.
	rec := do("GET", "/api/table", good, "http://localhost:3000", "", nil)
	if rec.Code != http.StatusForbidden || reached || rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("other localhost origin read = %d ACAO=%q, want 403 without CORS", rec.Code, rec.Header().Get("Access-Control-Allow-Origin"))
	}
	rec = do("OPTIONS", "/api/table/v2/update", good, "http://localhost:3000", "", nil)
	if rec.Code != http.StatusForbidden {
		t.Errorf("preflight from other origin = %d, want 403", rec.Code)
	}
	rec = do("OPTIONS", "/api/table/v2/update", good, self, "", nil)
	if rec.Code != http.StatusNoContent || rec.Header().Get("Access-Control-Allow-Origin") != self ||
		!strings.Contains(rec.Header().Get("Access-Control-Allow-Headers"), sessionHeader) {
		t.Errorf("own preflight = %d %v", rec.Code, rec.Header())
	}
	// Central gate: every mutating /api/ method needs the token, even on an
	// endpoint whose handler might forget.
	for _, m := range []string{"POST", "PUT", "PATCH", "DELETE"} {
		if rec := do(m, "/api/anything", good, self, "", nil); rec.Code != http.StatusForbidden || reached {
			t.Errorf("%s without token = %d reached=%v, want 403", m, rec.Code, reached)
		}
		if rec := do(m, "/api/anything", good, self, s.sessionToken, nil); rec.Code != http.StatusOK || !reached {
			t.Errorf("%s with token = %d, want 200", m, rec.Code)
		}
	}
	if rec := do("POST", "/api/anything", good, "", s.sessionToken, map[string]string{"Sec-Fetch-Site": "same-site"}); rec.Code != http.StatusForbidden {
		t.Errorf("same-site (other localhost port) fetch = %d, want 403", rec.Code)
	}
	if rec := do("POST", "/api/anything", good, "", s.sessionToken, map[string]string{"Sec-Fetch-Site": "same-origin"}); rec.Code != http.StatusOK {
		t.Errorf("same-origin fetch = %d, want 200", rec.Code)
	}
	// The explicitly configured dev origin (another localhost port, so the
	// browser marks it same-site) is allowed by its exact Origin.
	t.Setenv(devOriginEnv, "http://localhost:5173")
	if rec := do("POST", "/api/anything", good, "http://localhost:5173", s.sessionToken, map[string]string{"Sec-Fetch-Site": "same-site"}); rec.Code != http.StatusOK {
		t.Errorf("configured dev origin = %d, want 200", rec.Code)
	}
	if rec := do("POST", "/api/anything", good, "http://localhost:5174", s.sessionToken, map[string]string{"Sec-Fetch-Site": "same-site"}); rec.Code != http.StatusForbidden {
		t.Errorf("unconfigured neighbour port = %d, want 403", rec.Code)
	}
	// Non-API paths (the SPA) are not token-gated.
	if rec := do("GET", "/index.html", good, "", "", nil); rec.Code != http.StatusOK {
		t.Errorf("SPA asset = %d, want 200", rec.Code)
	}
}

func TestAuthErrorsAreMachineReadable(t *testing.T) {
	s := newAuthTestServer(t)
	check := func(origin, token, want string) {
		req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(`{}`))
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if token != "" {
			req.Header.Set(sessionHeader, token)
		}
		rec := httptest.NewRecorder()
		s.requireMutationAuth(rec, req)
		var body map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("auth error is not JSON: %q", rec.Body.String())
		}
		if rec.Code != http.StatusForbidden || body["auth"] != want {
			t.Errorf("origin=%q token=%q: %d %v, want 403 auth=%s", origin, token, rec.Code, body, want)
		}
	}
	check("http://localhost:1", s.sessionToken, "origin")
	check("", "stale-token", "session")
	check("http://localhost:59999", "", "session")
}

func TestSessionEndpointRefusesCrossSite(t *testing.T) {
	s := newAuthTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/session", nil)
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	rec := httptest.NewRecorder()
	s.handleSession(rec, req)
	if rec.Code != http.StatusForbidden || strings.Contains(rec.Body.String(), s.sessionToken) {
		t.Errorf("cross-site session read = %d %s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, "/api/session", nil)
	rec = httptest.NewRecorder()
	s.handleSession(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("same-origin session read = %d cache=%q", rec.Code, rec.Header().Get("Cache-Control"))
	}
}
