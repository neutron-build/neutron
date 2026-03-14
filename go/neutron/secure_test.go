package neutron

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSecureHeadersDefaults(t *testing.T) {
	mw := SecureHeaders(SecureHeadersOptions{})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	headers := map[string]string{
		"X-Content-Type-Options":    "nosniff",
		"X-Frame-Options":          "DENY",
		"Strict-Transport-Security": "max-age=63072000; includeSubDomains",
		"Referrer-Policy":          "strict-origin-when-cross-origin",
		"X-XSS-Protection":        "0",
	}

	for key, expected := range headers {
		got := w.Header().Get(key)
		if got != expected {
			t.Errorf("%s = %q, want %q", key, got, expected)
		}
	}
}

func TestSecureHeadersCustomValues(t *testing.T) {
	mw := SecureHeaders(SecureHeadersOptions{
		ContentTypeOptions:      "nosniff",
		FrameOptions:           "SAMEORIGIN",
		StrictTransportSecurity: "max-age=31536000",
		ReferrerPolicy:         "no-referrer",
		XSSProtection:          "1; mode=block",
		ContentSecurityPolicy:  "default-src 'self'",
		PermissionsPolicy:      "camera=(), microphone=()",
	})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Header().Get("X-Frame-Options") != "SAMEORIGIN" {
		t.Errorf("X-Frame-Options = %q", w.Header().Get("X-Frame-Options"))
	}
	if w.Header().Get("Content-Security-Policy") != "default-src 'self'" {
		t.Errorf("CSP = %q", w.Header().Get("Content-Security-Policy"))
	}
	if w.Header().Get("Permissions-Policy") != "camera=(), microphone=()" {
		t.Errorf("Permissions-Policy = %q", w.Header().Get("Permissions-Policy"))
	}
	if w.Header().Get("Strict-Transport-Security") != "max-age=31536000" {
		t.Errorf("HSTS = %q", w.Header().Get("Strict-Transport-Security"))
	}
}

func TestSecureHeadersDisableWithDash(t *testing.T) {
	mw := SecureHeaders(SecureHeadersOptions{
		FrameOptions:           "-", // disable
		StrictTransportSecurity: "-", // disable
	})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	// Disabled headers should NOT be set
	if w.Header().Get("X-Frame-Options") != "" {
		t.Errorf("X-Frame-Options should be absent, got %q", w.Header().Get("X-Frame-Options"))
	}
	if w.Header().Get("Strict-Transport-Security") != "" {
		t.Errorf("HSTS should be absent, got %q", w.Header().Get("Strict-Transport-Security"))
	}

	// Other headers should still be set with defaults
	if w.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q", w.Header().Get("X-Content-Type-Options"))
	}
}

func TestSecureHeadersNoCSPByDefault(t *testing.T) {
	mw := SecureHeaders(SecureHeadersOptions{})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Header().Get("Content-Security-Policy") != "" {
		t.Errorf("CSP should not be set by default, got %q", w.Header().Get("Content-Security-Policy"))
	}
	if w.Header().Get("Permissions-Policy") != "" {
		t.Errorf("Permissions-Policy should not be set by default, got %q", w.Header().Get("Permissions-Policy"))
	}
}

func TestSecureHeadersPassesThrough(t *testing.T) {
	mw := SecureHeaders(SecureHeadersOptions{})
	var called bool
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(200)
		w.Write([]byte("hello"))
	}))

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if !called {
		t.Error("next handler was not called")
	}
	if w.Body.String() != "hello" {
		t.Errorf("body = %q", w.Body.String())
	}
}

func TestSecureHeadersAppliedToAllMethods(t *testing.T) {
	mw := SecureHeaders(SecureHeadersOptions{})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	for _, method := range []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"} {
		t.Run(method, func(t *testing.T) {
			r := httptest.NewRequest(method, "/", nil)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)

			if w.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Errorf("%s: X-Content-Type-Options = %q", method, w.Header().Get("X-Content-Type-Options"))
			}
		})
	}
}

func TestSecureHeadersOptionsStruct(t *testing.T) {
	// Verify zero-value works
	opts := SecureHeadersOptions{}
	if opts.ContentTypeOptions != "" {
		t.Error("expected empty default")
	}
	if opts.FrameOptions != "" {
		t.Error("expected empty default")
	}
}
