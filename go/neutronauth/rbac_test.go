package neutronauth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRequireRolePasses(t *testing.T) {
	secret := "secret"
	token, _ := GenerateToken(Claims{"sub": "user1", "role": "admin"}, secret, time.Hour)

	handler := JWTMiddleware(secret)(
		RequireRole("admin", "superadmin")(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}),
		),
	)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestRequireRoleFails(t *testing.T) {
	secret := "secret"
	token, _ := GenerateToken(Claims{"sub": "user1", "role": "viewer"}, secret, time.Hour)

	handler := JWTMiddleware(secret)(
		RequireRole("admin")(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("should not reach handler")
			}),
		),
	)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
}

func TestRequireRoleNoClaims(t *testing.T) {
	handler := RequireRole("admin")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler")
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestRequirePermissionPasses(t *testing.T) {
	secret := "secret"
	token, _ := GenerateToken(Claims{
		"sub":         "user1",
		"permissions": []any{"read", "write", "delete"},
	}, secret, time.Hour)

	handler := JWTMiddleware(secret)(
		RequirePermission("read", "write")(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}),
		),
	)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestRequirePermissionFails(t *testing.T) {
	secret := "secret"
	token, _ := GenerateToken(Claims{
		"sub":         "user1",
		"permissions": []any{"read"},
	}, secret, time.Hour)

	handler := JWTMiddleware(secret)(
		RequirePermission("read", "write")(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("should not reach handler")
			}),
		),
	)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
}
