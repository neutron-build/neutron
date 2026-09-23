package neutronauth

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGenerateAndParseToken(t *testing.T) {
	secret := "test-secret-key-256-bits-long-enough!"
	claims := Claims{"sub": "user123", "role": "admin"}

	token, err := GenerateToken(claims, secret, time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	if token == "" {
		t.Fatal("empty token")
	}

	parsed, err := ParseToken(token, secret)
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	if parsed["sub"] != "user123" {
		t.Errorf("sub = %v", parsed["sub"])
	}
	if parsed["role"] != "admin" {
		t.Errorf("role = %v", parsed["role"])
	}
}

func TestParseTokenExpired(t *testing.T) {
	secret := "test-secret-0123456789abcdef0123456789abcdef"

	// GenerateToken refuses non-positive lifetimes (GO-15), so an expired
	// token is minted by signing one directly with a past exp.
	claims := Claims{"sub": "user123", "exp": time.Now().Add(-time.Hour).Unix()}
	payload, _ := json.Marshal(claims)
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + encoded
	token := signingInput + "." + sign(signingInput, secret)

	_, err := ParseToken(token, secret)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestParseTokenInvalidSignature(t *testing.T) {
	secret := "test-secret-0123456789abcdef0123456789abcdef"
	token, _ := GenerateToken(Claims{"sub": "user"}, secret, time.Hour)

	_, err := ParseToken(token, "wrong-secret")
	if err == nil {
		t.Fatal("expected error for wrong secret")
	}
}

func TestParseTokenInvalidFormat(t *testing.T) {
	_, err := ParseToken("not-a-jwt", "test-secret-0123456789abcdef0123456789abcdef")
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
}

func TestJWTMiddleware(t *testing.T) {
	secret := "test-secret-0123456789abcdef0123456789abcdef"
	token, _ := GenerateToken(Claims{"sub": "user123"}, secret, time.Hour)

	handler := JWTMiddleware(secret)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, err := ClaimsFromContext(r.Context())
		if err != nil {
			t.Error("missing claims in context")
		}
		if claims["sub"] != "user123" {
			t.Errorf("sub = %v", claims["sub"])
		}
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestJWTMiddlewareMissingHeader(t *testing.T) {
	handler := JWTMiddleware("test-secret-0123456789abcdef0123456789abcdef")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler")
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestJWTMiddlewareSkipPaths(t *testing.T) {
	handler := JWTMiddleware("test-secret-0123456789abcdef0123456789abcdef", WithSkipPaths("/health"))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/health", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (skipped path)", w.Code)
	}
}

func TestClaimsFromContextMissing(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	_, err := ClaimsFromContext(r.Context())
	if err == nil {
		t.Fatal("expected error for missing claims")
	}
}
