package neutronauth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAPIKeyMiddlewareValid(t *testing.T) {
	handler := APIKeyMiddleware(func(key string) (bool, error) {
		return key == "valid-key", nil
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-API-Key", "valid-key")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestAPIKeyMiddlewareInvalid(t *testing.T) {
	handler := APIKeyMiddleware(func(key string) (bool, error) {
		return key == "valid-key", nil
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler")
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-API-Key", "wrong-key")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestAPIKeyMiddlewareMissing(t *testing.T) {
	handler := APIKeyMiddleware(func(key string) (bool, error) {
		return true, nil
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler")
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestAPIKeyMiddlewareAuthorizationHeader(t *testing.T) {
	handler := APIKeyMiddleware(func(key string) (bool, error) {
		return key == "my-api-key", nil
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "ApiKey my-api-key")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}
