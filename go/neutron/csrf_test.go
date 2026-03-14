package neutron

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGenerateCSRFToken(t *testing.T) {
	token := generateCSRFToken()
	if len(token) != 64 { // 32 bytes hex-encoded
		t.Errorf("token length = %d, want 64", len(token))
	}

	// Verify uniqueness
	token2 := generateCSRFToken()
	if token == token2 {
		t.Error("tokens should be unique")
	}
}

func TestTokensMatch(t *testing.T) {
	tests := []struct {
		name  string
		a, b  string
		match bool
	}{
		{"identical", "abc123", "abc123", true},
		{"different", "abc123", "xyz789", false},
		{"empty_both", "", "", true},
		{"one_empty", "abc", "", false},
		{"different_length", "short", "longer", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tokensMatch(tc.a, tc.b)
			if got != tc.match {
				t.Errorf("tokensMatch(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.match)
			}
		})
	}
}

func TestIsUnsafeMethod(t *testing.T) {
	tests := []struct {
		method string
		unsafe bool
	}{
		{"GET", false},
		{"HEAD", false},
		{"OPTIONS", false},
		{"POST", true},
		{"PUT", true},
		{"PATCH", true},
		{"DELETE", true},
	}

	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			got := isUnsafeMethod(tc.method)
			if got != tc.unsafe {
				t.Errorf("isUnsafeMethod(%q) = %v, want %v", tc.method, got, tc.unsafe)
			}
		})
	}
}

func TestOriginInList(t *testing.T) {
	trusted := []string{"https://example.com", "https://app.example.com"}

	tests := []struct {
		origin string
		ok     bool
	}{
		{"https://example.com", true},
		{"https://example.com/", true},
		{"https://app.example.com", true},
		{"https://evil.com", false},
		{"http://example.com", false}, // scheme mismatch
		{"https://example.com/path/to/resource", true},
		{"", false},
	}

	for _, tc := range tests {
		t.Run(tc.origin, func(t *testing.T) {
			got := originInList(tc.origin, trusted)
			if got != tc.ok {
				t.Errorf("originInList(%q) = %v, want %v", tc.origin, got, tc.ok)
			}
		})
	}
}

func TestOriginInListEmpty(t *testing.T) {
	// Empty trusted list: no origin should match
	if originInList("https://example.com", nil) {
		t.Error("expected false for empty trusted list")
	}
}

func TestCSRFTokenFromContext(t *testing.T) {
	// Test with no token in context
	r := httptest.NewRequest("GET", "/", nil)
	token := CSRFTokenFromContext(r.Context())
	if token != "" {
		t.Errorf("expected empty token, got %q", token)
	}
}

func TestCSRFMiddlewareDefaults(t *testing.T) {
	opts := CSRFOptions{}
	mw := CSRF(opts)

	// Handler that echoes back the CSRF token from context
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := CSRFTokenFromContext(r.Context())
		w.Write([]byte(token))
	}))

	// GET request should set cookie and pass through
	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}

	// Verify CSRF cookie is set
	cookies := w.Result().Cookies()
	var csrfCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "__csrf" {
			csrfCookie = c
			break
		}
	}
	if csrfCookie == nil {
		t.Fatal("CSRF cookie not set")
	}
	if csrfCookie.SameSite != http.SameSiteStrictMode {
		t.Error("expected SameSite=Strict")
	}
	if csrfCookie.HttpOnly {
		t.Error("cookie should NOT be HttpOnly (SPAs need to read it)")
	}

	// Body should contain the token
	body := w.Body.String()
	if body == "" {
		t.Error("expected non-empty CSRF token in response body")
	}
}

func TestCSRFMiddlewarePOSTWithValidToken(t *testing.T) {
	opts := CSRFOptions{}
	mw := CSRF(opts)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))

	// First do a GET to get the token
	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	var token string
	for _, c := range w.Result().Cookies() {
		if c.Name == "__csrf" {
			token = c.Value
		}
	}
	if token == "" {
		t.Fatal("no CSRF token from GET")
	}

	// Now POST with the token in header
	r = httptest.NewRequest("POST", "/submit", nil)
	r.AddCookie(&http.Cookie{Name: "__csrf", Value: token})
	r.Header.Set("X-CSRF-Token", token)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestCSRFMiddlewarePOSTWithoutToken(t *testing.T) {
	opts := CSRFOptions{}
	mw := CSRF(opts)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("POST", "/submit", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
}

func TestCSRFMiddlewarePOSTWithFormField(t *testing.T) {
	opts := CSRFOptions{}
	mw := CSRF(opts)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	// GET to get token
	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	var token string
	for _, c := range w.Result().Cookies() {
		if c.Name == "__csrf" {
			token = c.Value
		}
	}

	// POST with token in form field
	r = httptest.NewRequest("POST", "/submit", strings.NewReader("_csrf="+token))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: "__csrf", Value: token})
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestCSRFMiddlewareSkipPaths(t *testing.T) {
	opts := CSRFOptions{
		SkipPaths: []string{"/api/webhook"},
	}
	mw := CSRF(opts)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	// POST to skipped path should pass without token
	r := httptest.NewRequest("POST", "/api/webhook/stripe", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200 (skipped path)", w.Code)
	}
}

func TestCSRFMiddlewarePOSTWrongToken(t *testing.T) {
	opts := CSRFOptions{}
	mw := CSRF(opts)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("POST", "/submit", nil)
	r.AddCookie(&http.Cookie{Name: "__csrf", Value: "real-token"})
	r.Header.Set("X-CSRF-Token", "wrong-token")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
}

func TestCSRFMiddlewareCustomOptions(t *testing.T) {
	opts := CSRFOptions{
		CookieName: "my_csrf",
		HeaderName: "X-My-Token",
		FormField:  "my_token",
		Path:       "/app",
	}
	mw := CSRF(opts)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	// GET to set cookie
	r := httptest.NewRequest("GET", "/app/page", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	var token string
	for _, c := range w.Result().Cookies() {
		if c.Name == "my_csrf" {
			token = c.Value
		}
	}
	if token == "" {
		t.Fatal("custom cookie name not set")
	}

	// POST with custom header
	r = httptest.NewRequest("POST", "/app/submit", nil)
	r.AddCookie(&http.Cookie{Name: "my_csrf", Value: token})
	r.Header.Set("X-My-Token", token)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestCSRFMiddlewareTrustedOrigins(t *testing.T) {
	opts := CSRFOptions{
		TrustedOrigins: []string{"https://app.example.com"},
	}
	mw := CSRF(opts)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	// GET to get token
	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	var token string
	for _, c := range w.Result().Cookies() {
		if c.Name == "__csrf" {
			token = c.Value
		}
	}

	// POST with untrusted origin
	r = httptest.NewRequest("POST", "/submit", nil)
	r.AddCookie(&http.Cookie{Name: "__csrf", Value: token})
	r.Header.Set("X-CSRF-Token", token)
	r.Header.Set("Origin", "https://evil.com")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403 for untrusted origin", w.Code)
	}

	// POST with trusted origin
	r = httptest.NewRequest("POST", "/submit", nil)
	r.AddCookie(&http.Cookie{Name: "__csrf", Value: token})
	r.Header.Set("X-CSRF-Token", token)
	r.Header.Set("Origin", "https://app.example.com")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200 for trusted origin", w.Code)
	}
}

func TestCSRFSafeMethodsAlwaysPass(t *testing.T) {
	opts := CSRFOptions{}
	mw := CSRF(opts)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	for _, method := range []string{"GET", "HEAD", "OPTIONS"} {
		t.Run(method, func(t *testing.T) {
			r := httptest.NewRequest(method, "/", nil)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != 200 {
				t.Errorf("%s status = %d, want 200", method, w.Code)
			}
		})
	}
}
