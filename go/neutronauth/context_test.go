package neutronauth

import (
	"context"
	"testing"
)

func TestWithClaimsAndRetrieval(t *testing.T) {
	claims := Claims{"user_id": float64(42), "role": "admin"}
	ctx := withClaims(context.Background(), claims)

	// Retrieve claims from context
	got, ok := ctx.Value(ctxKeyClaims).(Claims)
	if !ok {
		t.Fatal("claims not found in context")
	}
	if got["user_id"] != float64(42) {
		t.Errorf("user_id = %v, want 42", got["user_id"])
	}
	if got["role"] != "admin" {
		t.Errorf("role = %v, want admin", got["role"])
	}
}

func TestWithClaimsEmpty(t *testing.T) {
	claims := Claims{}
	ctx := withClaims(context.Background(), claims)

	got, ok := ctx.Value(ctxKeyClaims).(Claims)
	if !ok {
		t.Fatal("empty claims not found in context")
	}
	if len(got) != 0 {
		t.Errorf("expected empty claims, got %v", got)
	}
}

func TestWithClaimsOverwrite(t *testing.T) {
	claims1 := Claims{"user_id": float64(1)}
	ctx := withClaims(context.Background(), claims1)

	claims2 := Claims{"user_id": float64(2)}
	ctx = withClaims(ctx, claims2)

	got, ok := ctx.Value(ctxKeyClaims).(Claims)
	if !ok {
		t.Fatal("claims not found")
	}
	if got["user_id"] != float64(2) {
		t.Errorf("user_id = %v, want 2 (overwritten)", got["user_id"])
	}
}

func TestWithOAuthUserAndRetrieval(t *testing.T) {
	user := &OAuthUser{
		ID:       "12345",
		Email:    "alice@example.com",
		Name:     "Alice",
		Provider: "github",
	}
	ctx := WithOAuthUser(context.Background(), user)

	got := OAuthUserFromContext(ctx)
	if got == nil {
		t.Fatal("OAuthUser not found in context")
	}
	if got.ID != "12345" {
		t.Errorf("ID = %q, want 12345", got.ID)
	}
	if got.Email != "alice@example.com" {
		t.Errorf("Email = %q", got.Email)
	}
	if got.Name != "Alice" {
		t.Errorf("Name = %q", got.Name)
	}
	if got.Provider != "github" {
		t.Errorf("Provider = %q", got.Provider)
	}
}

func TestOAuthUserFromContextMissing(t *testing.T) {
	ctx := context.Background()
	got := OAuthUserFromContext(ctx)
	if got != nil {
		t.Errorf("expected nil OAuthUser from empty context, got %v", got)
	}
}

func TestOAuthUserFromContextWrongType(t *testing.T) {
	// Store a non-OAuthUser value at the key
	ctx := context.WithValue(context.Background(), ctxKeyOAuthUser, "not an OAuthUser")
	got := OAuthUserFromContext(ctx)
	if got != nil {
		t.Errorf("expected nil for wrong type, got %v", got)
	}
}

func TestWithOAuthUserNil(t *testing.T) {
	ctx := WithOAuthUser(context.Background(), nil)
	got := OAuthUserFromContext(ctx)
	if got != nil {
		t.Errorf("expected nil OAuthUser, got %v", got)
	}
}

func TestCtxKeysAreDistinct(t *testing.T) {
	// Ensure the context keys don't collide
	if ctxKeyClaims == ctxKeySession {
		t.Error("ctxKeyClaims and ctxKeySession should be distinct")
	}
	if ctxKeySession == ctxKeyOAuthUser {
		t.Error("ctxKeySession and ctxKeyOAuthUser should be distinct")
	}
	if ctxKeyClaims == ctxKeyOAuthUser {
		t.Error("ctxKeyClaims and ctxKeyOAuthUser should be distinct")
	}
}

func TestClaimsMapOperations(t *testing.T) {
	claims := Claims{
		"sub":   "user-123",
		"email": "test@example.com",
		"roles": []string{"admin"},
	}

	if claims["sub"] != "user-123" {
		t.Errorf("sub = %v", claims["sub"])
	}
	if claims["email"] != "test@example.com" {
		t.Errorf("email = %v", claims["email"])
	}

	// Test adding a field
	claims["custom"] = "value"
	if claims["custom"] != "value" {
		t.Errorf("custom = %v", claims["custom"])
	}
}
