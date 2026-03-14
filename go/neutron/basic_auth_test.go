package neutron

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBasicAuthValidCredentials(t *testing.T) {
	creds := map[string]string{
		"admin": "secret123",
		"user":  "pass456",
	}
	mw := BasicAuth("Test Realm", creds)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("authenticated"))
	}))

	r := httptest.NewRequest("GET", "/admin", nil)
	r.SetBasicAuth("admin", "secret123")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if w.Body.String() != "authenticated" {
		t.Errorf("body = %q", w.Body.String())
	}
}

func TestBasicAuthValidSecondUser(t *testing.T) {
	creds := map[string]string{
		"admin": "secret123",
		"user":  "pass456",
	}
	mw := BasicAuth("Test Realm", creds)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.SetBasicAuth("user", "pass456")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestBasicAuthWrongPassword(t *testing.T) {
	creds := map[string]string{"admin": "correct"}
	mw := BasicAuth("Realm", creds)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.SetBasicAuth("admin", "wrong")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestBasicAuthUnknownUser(t *testing.T) {
	creds := map[string]string{"admin": "secret"}
	mw := BasicAuth("Realm", creds)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.SetBasicAuth("unknown", "secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestBasicAuthNoCredentials(t *testing.T) {
	creds := map[string]string{"admin": "secret"}
	mw := BasicAuth("My Realm", creds)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}

	// Check WWW-Authenticate header
	auth := w.Header().Get("WWW-Authenticate")
	if auth == "" {
		t.Error("expected WWW-Authenticate header")
	}
	if auth != `Basic realm="My Realm"` {
		t.Errorf("WWW-Authenticate = %q", auth)
	}
}

func TestBasicAuthRFC7807ErrorBody(t *testing.T) {
	creds := map[string]string{"admin": "secret"}
	mw := BasicAuth("Realm", creds)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/protected", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != 401 {
		t.Fatalf("status = %d, want 401", w.Code)
	}

	// Verify RFC 7807 body
	ct := w.Header().Get("Content-Type")
	if ct == "" {
		t.Error("expected Content-Type header")
	}

	var pd ProblemDetail
	if err := json.Unmarshal(w.Body.Bytes(), &pd); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}
	if pd.Status != 401 {
		t.Errorf("pd.Status = %d, want 401", pd.Status)
	}
}

func TestBasicAuthEmptyCredentialsMap(t *testing.T) {
	creds := map[string]string{}
	mw := BasicAuth("Realm", creds)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	// Even with valid-looking auth, no users are in the map
	r := httptest.NewRequest("GET", "/", nil)
	r.SetBasicAuth("anyone", "anything")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestBasicAuthTimingSafety(t *testing.T) {
	// Verify that the middleware uses constant-time comparison.
	// We can't directly test timing, but we can verify the code path works
	// for both matching and non-matching passwords.
	creds := map[string]string{"admin": "longpassword123456"}
	mw := BasicAuth("Realm", creds)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	// Correct password
	r := httptest.NewRequest("GET", "/", nil)
	r.SetBasicAuth("admin", "longpassword123456")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Errorf("correct password: status = %d", w.Code)
	}

	// Wrong password (same length)
	r = httptest.NewRequest("GET", "/", nil)
	r.SetBasicAuth("admin", "wrongpassword12345")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Errorf("wrong password: status = %d", w.Code)
	}
}
